"""VMware VMDK disk export via OVF or HTTP download.

This module handles exporting VM disks from VMware to local files.
Two strategies are supported:

Strategy A (OVF Export) — Download via vSphere API HTTP lease
Strategy B (NBD/VDDK) — Stream via NBD protocol (future)

Confidence: 78 — OVF export is well-documented but has edge cases
with large disks and network timeouts.
"""

from __future__ import annotations

import os
import ssl
from pathlib import Path
from typing import Optional
from urllib.request import Request, urlopen

from pyVmomi import vim

from vmware2scw.utils.logging import get_logger
from vmware2scw.vmware.client import VSphereClient

logger = get_logger(__name__)


class VMExporter:
    """Export VM disks from VMware using the OVF export API.

    Uses the vSphere HTTP NFC lease mechanism to download VMDK files
    directly from the ESXi host.
    """

    def __init__(self, client: VSphereClient):
        self.client = client

    def export_vm_disks(
        self,
        vm_name: str,
        output_dir: Path,
        progress_callback=None,
    ) -> list[Path]:
        """Export all disks of a VM to local VMDK files."""
        import threading

        output_dir.mkdir(parents=True, exist_ok=True)

        container = self.client.get_container_view([vim.VirtualMachine])
        vm_obj = None
        for vm in container.view:
            if vm.name == vm_name:
                vm_obj = vm
                break
        container.Destroy()

        if vm_obj is None:
            raise ValueError(f"VM '{vm_name}' not found")

        logger.info(f"Starting OVF export for VM '{vm_name}'")

        lease = vm_obj.ExportVm()
        self._wait_for_lease(lease)

        # Shared progress state for heartbeat thread
        self._lease_progress = 0
        self._lease_done = False

        # Background thread to keep lease alive
        def _heartbeat():
            import time
            while not self._lease_done:
                try:
                    if lease.state == vim.HttpNfcLease.State.ready:
                        lease.HttpNfcLeaseProgress(self._lease_progress)
                except Exception:
                    pass
                time.sleep(20)

        heartbeat_thread = threading.Thread(target=_heartbeat, daemon=True)
        heartbeat_thread.start()

        exported_files = []
        try:
            total_disks = sum(1 for d in lease.info.deviceUrl if d.disk)
            disk_idx = 0

            for device_url in lease.info.deviceUrl:
                if not device_url.disk:
                    continue

                disk_key = device_url.key
                url = device_url.url

                if "*" in url:
                    url = url.replace("*", self.client._host)

                safe_key = disk_key.replace("/", "_").replace(":", "_").replace(" ", "_")
                file_name = f"{vm_name}-{safe_key}.vmdk"
                file_path = output_dir / file_name

                if file_path.exists():
                    logger.info(f"Disk file already exists, skipping: {file_name}")
                    exported_files.append(file_path)
                    disk_idx += 1
                    continue

                logger.info(f"Downloading disk: {file_name}")
                self._download_disk(url, file_path, lease, disk_idx, total_disks, progress_callback)
                exported_files.append(file_path)
                disk_idx += 1

            self._lease_done = True
            lease.HttpNfcLeaseComplete()
            logger.info(f"Export complete: {len(exported_files)} disk(s)")

        except Exception as e:
            self._lease_done = True
            try:
                lease.HttpNfcLeaseAbort()
            except Exception:
                pass
            raise RuntimeError(f"Export failed: {e}")

        return exported_files

    def _wait_for_lease(self, lease, timeout: int = 120) -> None:
        """Wait for an NFC lease to become ready."""
        import time
        start = time.time()
        while lease.state == vim.HttpNfcLease.State.initializing:
            if time.time() - start > timeout:
                raise TimeoutError("NFC lease timed out during initialization")
            time.sleep(2)

        if lease.state == vim.HttpNfcLease.State.error:
            raise RuntimeError(f"NFC lease error: {lease.error}")

        if lease.state != vim.HttpNfcLease.State.ready:
            raise RuntimeError(f"Unexpected lease state: {lease.state}")

    def _download_disk(
        self,
        url: str,
        output_path: Path,
        lease,
        disk_idx: int = 0,
        total_disks: int = 1,
        progress_callback=None,
        chunk_size: int = 16 * 1024 * 1024,  # 16MB chunks
    ) -> None:
        """Download a VMDK file from the NFC URL.

        Updates self._lease_progress for the heartbeat thread to send
        to vCenter, keeping the NFC lease alive.
        """
        import time

        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        req = Request(url)
        req.add_header("Content-Type", "application/x-vnd.vmware-streamVmdk")

        response = urlopen(req, context=ctx)
        total_size = int(response.headers.get("Content-Length", 0))

        downloaded = 0

        with open(output_path, "wb") as f:
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break

                f.write(chunk)
                downloaded += len(chunk)

                # Update global lease progress (0-100 across all disks)
                if total_size > 0:
                    disk_pct = downloaded / total_size
                    global_pct = int((disk_idx + disk_pct) / total_disks * 100)
                    self._lease_progress = min(global_pct, 99)

                if progress_callback and total_size > 0:
                    progress_callback(output_path.name, downloaded, total_size)

        if total_size > 0 and downloaded != total_size:
            logger.warning(
                f"Download size mismatch: expected {total_size}, got {downloaded}"
            )

        logger.info(f"Downloaded {output_path.name}: {downloaded / (1024**3):.2f} GB")
