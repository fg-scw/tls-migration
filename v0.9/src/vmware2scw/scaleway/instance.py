"""Scaleway API operations for image import and instance creation.

Uses the Block Storage API (v1) for snapshot import from S3,
and the Instance API for image/server creation.

Scaleway has deprecated b_ssd in the Instance API. All snapshot
imports now go through the Block Storage API endpoint:
POST /block/v1/zones/{zone}/snapshots/import-from-object-storage

Confidence: 85 — Based on current Scaleway documentation (Feb 2026).
"""

from __future__ import annotations

import time
from typing import Any, Optional

import requests

from vmware2scw.utils.logging import get_logger

logger = get_logger(__name__)

SCW_API_BASE = "https://api.scaleway.com"


class ScalewayInstanceAPI:
    """Interact with Scaleway APIs for importing images and creating instances."""

    def __init__(self, access_key: str, secret_key: str, project_id: str):
        self.project_id = project_id
        self.session = requests.Session()
        self.session.headers.update({
            "X-Auth-Token": secret_key,
            "Content-Type": "application/json",
        })

    def _url_block(self, zone: str, path: str) -> str:
        """Block Storage API URL."""
        return f"{SCW_API_BASE}/block/v1/zones/{zone}{path}"

    def _url_instance(self, zone: str, path: str) -> str:
        """Instance API URL."""
        return f"{SCW_API_BASE}/instance/v1/zones/{zone}{path}"

    def _request(self, method: str, url: str, **kwargs) -> dict[str, Any]:
        resp = self.session.request(method, url, **kwargs)
        if not resp.ok:
            logger.error(f"API error {resp.status_code}: {resp.text[:500]}")
            resp.raise_for_status()
        if resp.status_code == 204:
            return {}
        return resp.json()

    # ── Snapshots (Block Storage API) ────────────────────────────

    def create_snapshot_from_s3(
        self,
        zone: str,
        name: str,
        bucket: str,
        key: str,
        size: Optional[int] = None,
    ) -> dict:
        """Import a qcow2 from S3 as a Block Storage snapshot.

        Uses: POST /block/v1/zones/{zone}/snapshots/import-from-object-storage

        Args:
            zone: Scaleway zone (e.g. "fr-par-1")
            name: Snapshot name
            bucket: S3 bucket name (must be same region)
            key: S3 object key
            size: Volume size in bytes (optional, defaults to qcow2 virtual size)
        """
        url = self._url_block(zone, "/snapshots/import-from-object-storage")

        payload: dict[str, Any] = {
            "name": name,
            "project_id": self.project_id,
            "bucket": bucket,
            "key": key,
        }
        if size:
            payload["size"] = size

        logger.info(f"Importing snapshot via Block Storage API from s3://{bucket}/{key}")
        result = self._request("POST", url, json=payload)
        snapshot = result.get("snapshot", result)
        logger.info(f"Snapshot import initiated: {snapshot.get('id', 'unknown')} "
                     f"(status: {snapshot.get('status', 'unknown')})")
        return snapshot

    def get_snapshot(self, zone: str, snapshot_id: str) -> dict:
        """Get Block Storage snapshot details."""
        url = self._url_block(zone, f"/snapshots/{snapshot_id}")
        result = self._request("GET", url)
        return result.get("snapshot", result)

    def wait_for_snapshot(
        self,
        zone: str,
        snapshot_id: str,
        timeout: int = 1800,
        poll_interval: int = 15,
    ) -> dict:
        """Wait for snapshot import to complete.

        Import can take a while for large images (10-30min for multi-GB).
        """
        start = time.time()
        while True:
            snapshot = self.get_snapshot(zone, snapshot_id)
            status = snapshot.get("status", "unknown")

            if status == "available":
                logger.info(f"Snapshot {snapshot_id} is available")
                return snapshot
            elif status in ("error", "in_error"):
                raise RuntimeError(f"Snapshot {snapshot_id} failed: {status}")

            elapsed = time.time() - start
            if elapsed > timeout:
                raise TimeoutError(
                    f"Snapshot {snapshot_id} not ready after {timeout}s (status: {status})"
                )

            logger.info(f"Snapshot status: {status} ({elapsed:.0f}s elapsed)")
            time.sleep(poll_interval)

    # ── Images (Instance API) ────────────────────────────────────

    def create_image(
        self,
        zone: str,
        name: str,
        root_volume_snapshot_id: str,
        arch: str = "x86_64",
    ) -> dict:
        """Create an Instance image from a Block Storage snapshot.

        Note: The snapshot must be 'available' before creating an image.
        """
        url = self._url_instance(zone, "/images")
        payload = {
            "name": name,
            "project": self.project_id,
            "root_volume": root_volume_snapshot_id,
            "arch": arch,
        }

        logger.info(f"Creating image '{name}' from snapshot {root_volume_snapshot_id}")
        result = self._request("POST", url, json=payload)
        image = result.get("image", result)
        logger.info(f"Image created: {image.get('id', 'unknown')}")
        return image

    def get_image(self, zone: str, image_id: str) -> dict:
        url = self._url_instance(zone, f"/images/{image_id}")
        result = self._request("GET", url)
        return result.get("image", result)

    # ── Instances ────────────────────────────────────────────────

    def create_instance(
        self,
        zone: str,
        name: str,
        image_id: str,
        commercial_type: str,
        tags: Optional[list[str]] = None,
    ) -> dict:
        url = self._url_instance(zone, "/servers")
        payload = {
            "name": name,
            "project": self.project_id,
            "image": image_id,
            "commercial_type": commercial_type,
        }
        if tags:
            payload["tags"] = tags

        logger.info(f"Creating instance '{name}' ({commercial_type})")
        result = self._request("POST", url, json=payload)
        server = result.get("server", result)
        logger.info(f"Instance created: {server.get('id', 'unknown')}")
        return server

    def list_instance_types(self, zone: str) -> dict:
        url = self._url_instance(zone, "/products/servers")
        return self._request("GET", url).get("servers", {})
