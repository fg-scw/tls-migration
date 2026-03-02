"""Migration pipeline orchestrator — coordinates all migration stages."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from vmware2scw.config import AppConfig, VMMigrationPlan
from vmware2scw.pipeline.state import MigrationState, MigrationStateStore
from vmware2scw.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class MigrationResult:
    """Result of a migration execution."""
    success: bool
    migration_id: str
    vm_name: str
    instance_id: Optional[str] = None
    image_id: Optional[str] = None
    duration: str = ""
    failed_stage: Optional[str] = None
    error: Optional[str] = None
    completed_stages: list[str] = field(default_factory=list)


class MigrationPipeline:
    """Orchestrates the full VMware → Scaleway migration pipeline.

    Stages (executed in order):
    1. validate       — Pre-flight compatibility checks
    2. snapshot       — Create VMware snapshot for consistency
    3. export         — Export VMDK disks from VMware
    4. clean_tools    — Remove VMware tools from guest
    5. inject_virtio  — Inject VirtIO drivers for KVM
    6. convert        — Convert VMDK → qcow2
    7. fix_bootloader — Adapt bootloader for KVM (fstab, GRUB, initramfs)
    8. fix_network    — Adapt network configuration
    9. upload_s3      — Upload qcow2 to Scaleway Object Storage
    10. import_scw    — Import image into Scaleway (snapshot → image)
    11. verify        — Post-migration health checks
    12. cleanup       — Remove temporary files, snapshots

    Each stage is idempotent and can be resumed after failure.

    Confidence: 88 — Pipeline pattern is proven; individual stage
    confidence varies (see DESIGN.md for details).
    """

    STAGES = [
        "validate",
        "snapshot",
        "export",
        "convert",           # MUST be before clean_tools: exported VMDK is streamOptimized, unreadable by libguestfs
        "clean_tools",
        "inject_virtio",
        "fix_bootloader",
        "ensure_uefi",       # Convert BIOS→UEFI if needed (after bootloader fix)
        "fix_network",
        "upload_s3",
        "import_scw",
        "verify",
        "cleanup",
    ]

    def __init__(self, config: AppConfig):
        self.config = config
        self.state_store = MigrationStateStore(config.conversion.work_dir)

    def run(self, plan: VMMigrationPlan) -> MigrationResult:
        """Execute a full migration for a single VM.

        Args:
            plan: Migration plan with VM name, target type, etc.

        Returns:
            MigrationResult with success status and details
        """
        migration_id = str(uuid.uuid4())[:8]
        start_time = time.time()

        state = MigrationState(
            migration_id=migration_id,
            vm_name=plan.vm_name,
            target_type=plan.target_type,
            zone=plan.zone,
            current_stage="",
            completed_stages=[],
            artifacts={},
            started_at=datetime.now(),
        )
        self.state_store.save(state)

        logger.info(f"[bold]Starting migration {migration_id}[/bold]: "
                     f"{plan.vm_name} → {plan.target_type} ({plan.zone})")

        stages_to_run = self.STAGES
        if plan.skip_validation:
            stages_to_run = [s for s in stages_to_run if s != "validate"]

        for stage_name in stages_to_run:
            state.current_stage = stage_name
            self.state_store.save(state)

            logger.info(f"[cyan]▶ Stage: {stage_name}[/cyan]")
            try:
                self._execute_stage(stage_name, plan, state)
                state.completed_stages.append(stage_name)
                self.state_store.save(state)
                logger.info(f"[green]✓ Stage {stage_name} complete[/green]")

            except Exception as e:
                elapsed = time.time() - start_time
                state.error = str(e)
                self.state_store.save(state)

                logger.error(f"[red]✗ Stage {stage_name} failed: {e}[/red]")
                return MigrationResult(
                    success=False,
                    migration_id=migration_id,
                    vm_name=plan.vm_name,
                    failed_stage=stage_name,
                    error=str(e),
                    duration=f"{elapsed:.0f}s",
                    completed_stages=list(state.completed_stages),
                )

        elapsed = time.time() - start_time
        logger.info(f"[bold green]Migration {migration_id} complete in {elapsed:.0f}s[/bold green]")

        return MigrationResult(
            success=True,
            migration_id=migration_id,
            vm_name=plan.vm_name,
            instance_id=state.artifacts.get("scaleway_instance_id"),
            image_id=state.artifacts.get("scaleway_image_id"),
            duration=f"{elapsed:.0f}s",
            completed_stages=list(state.completed_stages),
        )

    def resume(self, migration_id: str) -> MigrationResult:
        """Resume a failed migration from the last successful stage."""
        state = self.state_store.load(migration_id)
        if not state:
            raise ValueError(f"Migration '{migration_id}' not found")

        logger.info(f"Resuming migration {migration_id} for VM '{state.vm_name}'")
        logger.info(f"Completed stages: {', '.join(state.completed_stages)}")

        plan = VMMigrationPlan(
            vm_name=state.vm_name,
            target_type=state.target_type,
            zone=state.zone,
        )

        # Find remaining stages
        remaining = [s for s in self.STAGES if s not in state.completed_stages]
        if not remaining:
            return MigrationResult(
                success=True,
                migration_id=migration_id,
                vm_name=state.vm_name,
                completed_stages=list(state.completed_stages),
            )

        start_time = time.time()
        state.error = None

        for stage_name in remaining:
            state.current_stage = stage_name
            self.state_store.save(state)

            logger.info(f"[cyan]▶ Stage: {stage_name}[/cyan] (resumed)")
            try:
                self._execute_stage(stage_name, plan, state)
                state.completed_stages.append(stage_name)
                self.state_store.save(state)
                logger.info(f"[green]✓ Stage {stage_name} complete[/green]")

            except Exception as e:
                state.error = str(e)
                self.state_store.save(state)
                elapsed = time.time() - start_time

                return MigrationResult(
                    success=False,
                    migration_id=migration_id,
                    vm_name=state.vm_name,
                    failed_stage=stage_name,
                    error=str(e),
                    duration=f"{elapsed:.0f}s",
                    completed_stages=list(state.completed_stages),
                )

        elapsed = time.time() - start_time
        return MigrationResult(
            success=True,
            migration_id=migration_id,
            vm_name=state.vm_name,
            instance_id=state.artifacts.get("scaleway_instance_id"),
            image_id=state.artifacts.get("scaleway_image_id"),
            duration=f"{elapsed:.0f}s",
            completed_stages=list(state.completed_stages),
        )

    def dry_run(self, plan: VMMigrationPlan) -> None:
        """Simulate a migration without executing any stages."""
        logger.info(f"[yellow]DRY RUN for VM '{plan.vm_name}'[/yellow]")
        logger.info(f"Target: {plan.target_type} in {plan.zone}")
        logger.info(f"Stages that would execute:")
        for i, stage in enumerate(self.STAGES, 1):
            if plan.skip_validation and stage == "validate":
                logger.info(f"  {i}. {stage} [dim](skipped)[/dim]")
            else:
                logger.info(f"  {i}. {stage}")

    def _execute_stage(self, stage: str, plan: VMMigrationPlan, state: MigrationState) -> None:
        """Execute a single pipeline stage.

        Each stage method updates state.artifacts with any intermediate
        results (file paths, IDs, etc.) for use by subsequent stages.
        """
        handler = getattr(self, f"_stage_{stage}", None)
        if handler is None:
            raise NotImplementedError(f"Stage '{stage}' not implemented yet")
        handler(plan, state)

    # ─── Stage implementations ───────────────────────────────────────

    def _stage_validate(self, plan: VMMigrationPlan, state: MigrationState) -> None:
        """Pre-flight validation: check VM compatibility with target type."""
        from vmware2scw.pipeline.validator import MigrationValidator
        from vmware2scw.vmware.client import VSphereClient
        from vmware2scw.vmware.inventory import VMInventory

        client = VSphereClient()
        pw = self.config.vmware.password.get_secret_value() if self.config.vmware.password else ""
        client.connect(
            self.config.vmware.vcenter,
            self.config.vmware.username,
            pw,
            insecure=self.config.vmware.insecure,
        )

        inv = VMInventory(client)
        vm_info = inv.get_vm_info(plan.vm_name)
        state.artifacts["vm_info"] = vm_info.model_dump()

        # Log VM characteristics for debugging
        firmware = vm_info.firmware if hasattr(vm_info, 'firmware') else 'unknown'
        guest_os = vm_info.guest_os if hasattr(vm_info, 'guest_os') else 'unknown'
        logger.info(f"VM '{plan.vm_name}': guest_os={guest_os}, firmware={firmware}, "
                     f"cpu={vm_info.cpu}, ram={vm_info.memory_mb}MB, "
                     f"disks={len(vm_info.disks)}")
        if firmware == "efi":
            logger.info("  Source VM uses UEFI firmware (Scaleway also uses UEFI — good)")
        else:
            logger.info("  Source VM uses BIOS firmware (will convert to UEFI for Scaleway)")

        validator = MigrationValidator()
        report = validator.validate(vm_info, plan.target_type)

        client.disconnect()

        if not report.passed:
            failures = [c for c in report.checks if not c.passed and c.blocking]
            msg = "; ".join(f"{c.name}: {c.message}" for c in failures)
            raise RuntimeError(f"Pre-validation failed: {msg}")

    def _stage_snapshot(self, plan: VMMigrationPlan, state: MigrationState) -> None:
        """Create a VMware snapshot for consistent export."""
        from vmware2scw.vmware.client import VSphereClient
        from vmware2scw.vmware.snapshot import SnapshotManager

        client = VSphereClient()
        pw = self.config.vmware.password.get_secret_value() if self.config.vmware.password else ""
        client.connect(
            self.config.vmware.vcenter,
            self.config.vmware.username,
            pw,
            insecure=self.config.vmware.insecure,
        )

        snap_mgr = SnapshotManager(client)
        snap_name = f"vmware2scw-{state.migration_id}"
        snap_mgr.create_migration_snapshot(plan.vm_name, snap_name)
        state.artifacts["snapshot_name"] = snap_name

        client.disconnect()

    def _stage_export(self, plan: VMMigrationPlan, state: MigrationState) -> None:
        """Export VMDK disks from VMware."""
        from vmware2scw.vmware.client import VSphereClient
        from vmware2scw.vmware.export import VMExporter

        work_dir = self.config.conversion.work_dir / state.migration_id
        work_dir.mkdir(parents=True, exist_ok=True)

        client = VSphereClient()
        pw = self.config.vmware.password.get_secret_value() if self.config.vmware.password else ""
        client.connect(
            self.config.vmware.vcenter,
            self.config.vmware.username,
            pw,
            insecure=self.config.vmware.insecure,
        )

        exporter = VMExporter(client)
        vmdk_paths = exporter.export_vm_disks(plan.vm_name, work_dir)
        state.artifacts["vmdk_paths"] = [str(p) for p in vmdk_paths]

        client.disconnect()

    def _stage_clean_tools(self, plan: VMMigrationPlan, state: MigrationState) -> None:
        """Clean VMware tools from converted qcow2 disks.

        Only processes the boot disk (first disk). Additional data disks
        don't contain an OS and would fail virt-customize inspection.
        """
        from vmware2scw.converter.disk import VMwareToolsCleaner
        from vmware2scw.scaleway.mapping import ResourceMapper

        mapper = ResourceMapper()
        vm_info_dict = state.artifacts.get("vm_info", {})
        guest_os = vm_info_dict.get("guest_os", "otherLinux64Guest")
        os_family, _ = mapper.get_os_family(guest_os)

        qcow2_paths = state.artifacts.get("qcow2_paths", [])
        if not qcow2_paths:
            logger.warning("No qcow2 files found — skipping clean_tools")
            return

        cleaner = VMwareToolsCleaner()
        # Only clean the boot disk (first disk)
        boot_disk = qcow2_paths[0]
        logger.info(f"Cleaning boot disk: {Path(boot_disk).name}")
        cleaner.clean(boot_disk, os_family=os_family)

        if len(qcow2_paths) > 1:
            logger.info(f"Skipping {len(qcow2_paths) - 1} data disk(s) — no OS to clean")

    def _stage_inject_virtio(self, plan: VMMigrationPlan, state: MigrationState) -> None:
        """Use virt-v2v to prepare the image for KVM boot.

        Based on:
        - Scaleway doc (Windows): virt-v2v -i disk <q> -block-driver virtio-scsi -o qemu -os ./out
        - migrate_centos.sh: virt-v2v -i disk <q> -o qemu -on <n> -os <dir> -of qcow2 -oc qcow2

        virt-v2v handles: VirtIO drivers, bootloader, initramfs, BIOS/UEFI.
        """
        import shutil
        import subprocess
        from vmware2scw.scaleway.mapping import ResourceMapper
        from vmware2scw.utils.subprocess import run_command, check_tool_available

        mapper = ResourceMapper()
        vm_info_dict = state.artifacts.get("vm_info", {})
        guest_os = vm_info_dict.get("guest_os", "otherLinux64Guest")
        os_family, _ = mapper.get_os_family(guest_os)

        qcow2_paths = state.artifacts.get("qcow2_paths", [])
        if not qcow2_paths:
            logger.warning("No qcow2 files found — skipping inject_virtio")
            return

        boot_disk = Path(qcow2_paths[0])

        # ──── Windows: Phase 1 → virt-v2v → Phase 2 ────
        # CRITICAL ordering:
        # - Phase 1 runs on the ORIGINAL qcow2 (non-compressed, writable)
        #   to stage drivers, setup script, and registry entries.
        # - virt-v2v runs next for PCI device binding (required for boot).
        #   virt-v2v preserves our staged files but its output is NOT writable
        #   (NTFS dirty flag from virt-v2v's internal modifications).
        # - Phase 2 boots the virt-v2v output in QEMU (virtio-blk).
        #   Windows runs pnputil to install all drivers into DriverStore.
        #   QEMU writes directly to the qcow2 — NTFS dirty is fine.
        if os_family == "windows":
            virtio_iso = self.config.conversion.virtio_win_iso
            if not virtio_iso or not Path(virtio_iso).exists():
                raise RuntimeError(
                    "virtio-win ISO is required for Windows VMs.\n"
                    "  wget -O /opt/virtio-win.iso "
                    "https://fedorapeople.org/groups/virt/virtio-win/direct-downloads/stable-virtio/virtio-win.iso\n"
                    "  Then in migration.yaml: conversion.virtio_win_iso: /opt/virtio-win.iso"
                )

            self._ensure_rhsrvany()

            # Step 1: Phase 1 — offline prep on ORIGINAL writable qcow2
            logger.info("Windows Step 1/3: Offline driver staging (Phase 1)...")
            from vmware2scw.converter.windows_virtio import _phase1_offline, _phase2_qemu_boot, ensure_prerequisites
            import tempfile
            ensure_prerequisites()
            p1_work = boot_disk.parent / "virtio-phase1"
            p1_work.mkdir(parents=True, exist_ok=True)
            _phase1_offline(str(boot_disk), str(virtio_iso), p1_work)

            # Step 2: virt-v2v — PCI device binding
            logger.info("Windows Step 2/3: virt-v2v (PCI device binding)...")
            env = {"LIBGUESTFS_BACKEND": "direct", "VIRTIO_WIN": str(virtio_iso)}
            out_dir = boot_disk.parent / "v2v-out"
            out_dir.mkdir(parents=True, exist_ok=True)
            v2v_name = f"v2v-{boot_disk.stem}"

            v2v_ok = False
            for i, cmd in enumerate([
                ["virt-v2v", "-i", "disk", str(boot_disk),
                 "-o", "qemu", "-os", str(out_dir),
                 "-on", v2v_name, "-of", "qcow2", "-oc", "qcow2"],
                ["virt-v2v", "-i", "disk", str(boot_disk),
                 "-o", "local", "-os", str(out_dir),
                 "-on", v2v_name, "-of", "qcow2"],
            ], 1):
                logger.info(f"  Trying virt-v2v syntax {i}/2...")
                try:
                    run_command(cmd, env=env, timeout=3600)
                    v2v_ok = True
                    logger.info(f"  virt-v2v syntax {i} succeeded")
                    break
                except Exception as e:
                    logger.warning(f"  virt-v2v syntax {i} failed: {e}")
                    for f in out_dir.iterdir():
                        f.unlink(missing_ok=True)

            if not v2v_ok:
                raise RuntimeError("virt-v2v failed — cannot prepare Windows for KVM")

            # Find virt-v2v output and replace boot disk
            candidates = sorted(
                [f for f in out_dir.iterdir()
                 if f.is_file() and f.stat().st_size > 1024 * 1024
                 and f.suffix not in ('.xml', '.sh')],
                key=lambda f: f.stat().st_size, reverse=True,
            )
            if not candidates:
                raise RuntimeError(f"virt-v2v produced no output in {out_dir}")

            converted = candidates[0]
            logger.info(f"  virt-v2v output: {converted.name} ({converted.stat().st_size / (1024**3):.1f} GB)")

            boot_disk.unlink(missing_ok=True)
            import shutil as _shutil
            _shutil.move(str(converted), str(boot_disk))
            _shutil.rmtree(out_dir, ignore_errors=True)
            state.artifacts["qcow2_paths"][0] = str(boot_disk)
            logger.info("  virt-v2v complete — boot disk replaced")

            # Step 3: Phase 2 — QEMU boot for pnputil
            logger.info("Windows Step 3/4: QEMU virtio-blk boot (pnputil driver installation)...")
            p2_work = boot_disk.parent / "virtio-phase2"
            p2_work.mkdir(parents=True, exist_ok=True)
            phase2_ok = _phase2_qemu_boot(str(boot_disk), p2_work)

            if not phase2_ok:
                logger.warning("Phase 2 QEMU boot may have timed out — checking if drivers installed anyway")

            # Step 4: Phase 3 — QEMU dual boot (virtio-blk + virtio-scsi PnP binding)
            # Scaleway uses virtio-scsi. Phase 2 installed vioscsi in DriverStore,
            # but Windows needs to see the virtio-scsi PCI device to bind the driver.
            # We boot with BOTH controllers: virtio-blk (to boot) + virtio-scsi (for PnP).
            # Windows auto-detects virtio-scsi and binds vioscsi from DriverStore.
            logger.info("Windows Step 4/4: QEMU dual boot (virtio-scsi PnP binding)...")
            from vmware2scw.converter.windows_virtio import _phase3_dual_boot
            p3_work = boot_disk.parent / "virtio-phase3"
            p3_work.mkdir(parents=True, exist_ok=True)
            _phase3_dual_boot(str(boot_disk), p3_work)

            return

        # ──── Linux: use virt-v2v ────
            logger.warning("virt-v2v not installed — using virt-customize fallback")
            self._inject_virtio_fallback(boot_disk, os_family)
            return

        # Setup environment
        env = {"LIBGUESTFS_BACKEND": "direct"}
        mounted_virtio = False

        if os_family == "windows":
            virtio_iso = self.config.conversion.virtio_win_iso
            if not virtio_iso or not Path(virtio_iso).exists():
                raise RuntimeError(
                    "virtio-win ISO is required for Windows VMs.\n"
                    "  wget -O /opt/virtio-win.iso "
                    "https://fedorapeople.org/groups/virt/virtio-win/direct-downloads/stable-virtio/virtio-win.iso\n"
                    "  Then in migration.yaml: conversion.virtio_win_iso: /opt/virtio-win.iso"
                )
            # Point VIRTIO_WIN directly to the ISO (virt-v2v accepts both dir and ISO)
            env["VIRTIO_WIN"] = str(virtio_iso)

            # Ensure rhsrvany.exe is installed (required by virt-v2v on Ubuntu/Debian)
            self._ensure_rhsrvany()

        # Output directory
        out_dir = boot_disk.parent / "v2v-out"
        out_dir.mkdir(parents=True, exist_ok=True)
        v2v_name = f"v2v-{boot_disk.stem}"

        # Try multiple virt-v2v syntaxes (varies by distro/version)
        v2v_syntaxes = [
            # Syntax 1: migrate_centos.sh style (-o qemu -of qcow2 -oc qcow2)
            ["virt-v2v", "-i", "disk", str(boot_disk),
             "-o", "qemu", "-os", str(out_dir),
             "-on", v2v_name, "-of", "qcow2", "-oc", "qcow2"],
            # Syntax 2: Scaleway doc style (--block-driver)
            ["virt-v2v", "-i", "disk", str(boot_disk),
             "-o", "qemu", "-os", str(out_dir),
             "-on", v2v_name, "-of", "qcow2",
             "--block-driver", "virtio-scsi"],
            # Syntax 3: -o local (most compatible)
            ["virt-v2v", "-i", "disk", str(boot_disk),
             "-o", "local", "-os", str(out_dir),
             "-on", v2v_name, "-of", "qcow2"],
        ]

        v2v_ok = False
        for i, cmd in enumerate(v2v_syntaxes, 1):
            logger.info(f"Trying virt-v2v syntax {i}/{len(v2v_syntaxes)}...")
            try:
                run_command(cmd, env=env, timeout=3600)
                v2v_ok = True
                logger.info(f"virt-v2v syntax {i} succeeded")
                break
            except Exception as e:
                logger.warning(f"virt-v2v syntax {i} failed: {e}")
                for f in out_dir.iterdir():
                    f.unlink(missing_ok=True)

        if not v2v_ok:
            logger.warning("All virt-v2v syntaxes failed — using virt-customize fallback")
            self._inject_virtio_fallback(boot_disk, os_family)
            return

        # Find the virt-v2v output (named <v2v_name>-sda or similar)
        candidates = sorted(
            [f for f in out_dir.iterdir()
             if f.is_file() and f.stat().st_size > 1024 * 1024
             and f.suffix not in ('.xml', '.sh')],
            key=lambda f: f.stat().st_size, reverse=True,
        )
        if not candidates:
            raise RuntimeError(f"virt-v2v succeeded but no output in {out_dir}")

        converted = candidates[0]
        logger.info(f"virt-v2v output: {converted.name} ({converted.stat().st_size / (1024**3):.2f} GB)")

        # Linux: restore original fstab (virt-v2v overrides UUIDs with /dev/sda*)
        if os_family == "linux":
            logger.info("Restoring original fstab (virt-v2v may have replaced UUIDs)...")
            try:
                run_command([
                    "virt-customize", "-a", str(converted),
                    "--run-command",
                    "if [ -f /etc/fstab.augsave ]; then cp /etc/fstab.augsave /etc/fstab; echo Restored; fi",
                ], env={"LIBGUESTFS_BACKEND": "direct"})
            except Exception as e:
                logger.warning(f"fstab restore failed (non-critical): {e}")

        # Ensure output is qcow2
        import json as _json
        info_out = run_command(["qemu-img", "info", "--output=json", str(converted)], capture_output=True)
        fmt = _json.loads(info_out.stdout).get("format", "raw")
        if fmt != "qcow2":
            logger.info(f"Converting virt-v2v output from {fmt} to qcow2...")
            final_qcow2 = out_dir / "boot-v2v.qcow2"
            compress = ["-c"] if self.config.conversion.compress_qcow2 else []
            run_command(["qemu-img", "convert", "-O", "qcow2"] + compress + [str(converted), str(final_qcow2)])
            converted = final_qcow2

        # Replace original boot disk — clean up to save space
        boot_disk.unlink(missing_ok=True)
        shutil.move(str(converted), str(boot_disk))
        shutil.rmtree(out_dir, ignore_errors=True)
        logger.info("virt-v2v conversion complete — boot disk replaced")

        # Linux post-processing only (Windows is handled above)

    def _fix_ntfs_dirty_flag(self, qcow2_path):
        """Fix NTFS dirty flag that prevents write access.

        Windows Hibernation and Fast Startup leave the NTFS filesystem in a
        'dirty' state. virt-v2v and guestfish will refuse to mount read-write.

        Uses qemu-nbd + host ntfsfix for maximum reliability.
        """
        import os
        import subprocess
        import time

        logger.info("Checking/fixing NTFS dirty flag (Fast Startup / Hibernation)...")
        gf_env = {**os.environ, "LIBGUESTFS_BACKEND": "direct"}

        # Method 1: qemu-nbd + ntfsfix (most reliable)
        nbd_dev = "/dev/nbd0"
        subprocess.run(["modprobe", "nbd", "max_part=8"], capture_output=True)
        subprocess.run(["qemu-nbd", "--disconnect", nbd_dev], capture_output=True)

        r = subprocess.run(
            ["qemu-nbd", "--connect", nbd_dev, str(qcow2_path)],
            capture_output=True, text=True,
        )
        if r.returncode == 0:
            try:
                time.sleep(1)
                for i in range(1, 8):
                    part = f"{nbd_dev}p{i}"
                    if not os.path.exists(part):
                        continue
                    blkid_r = subprocess.run(
                        ["blkid", "-o", "value", "-s", "TYPE", part],
                        capture_output=True, text=True,
                    )
                    if "ntfs" in blkid_r.stdout.lower():
                        logger.info(f"  Running ntfsfix -d on {part}...")
                        fix_r = subprocess.run(
                            ["ntfsfix", "-d", part],
                            capture_output=True, text=True,
                        )
                        if fix_r.returncode == 0:
                            logger.info(f"  ntfsfix succeeded on {part}")
                        else:
                            logger.warning(f"  ntfsfix on {part}: {fix_r.stderr.strip()[:200]}")
            finally:
                subprocess.run(["qemu-nbd", "--disconnect", nbd_dev], capture_output=True)
        else:
            logger.warning(f"  qemu-nbd not available: {r.stderr.strip()[:200]}")

        # Method 2: Disable Fast Startup via hivex
        try:
            r2 = subprocess.run(
                ["guestfish", "-a", str(qcow2_path), "-i", "--",
                 "download", "/Windows/System32/config/SYSTEM", "/tmp/SYSTEM.ntfsfix"],
                capture_output=True, text=True, env=gf_env,
            )
            if r2.returncode == 0:
                from pathlib import Path as P
                reg_content = (
                    'Windows Registry Editor Version 5.00\n\n'
                    '[HKEY_LOCAL_MACHINE\\\\SYSTEM\\\\ControlSet001\\\\Control\\\\Session Manager\\\\Power]\n'
                    '"HiberbootEnabled"=dword:00000000\n'
                )
                P("/tmp/disable-fastboot.reg").write_text(reg_content)
                subprocess.run(
                    ["hivexregedit", "--merge", "/tmp/SYSTEM.ntfsfix",
                     "--prefix", "HKEY_LOCAL_MACHINE\\SYSTEM",
                     "/tmp/disable-fastboot.reg"],
                    capture_output=True, text=True,
                )
                subprocess.run(
                    ["guestfish", "-a", str(qcow2_path), "-i", "--",
                     "upload", "/tmp/SYSTEM.ntfsfix",
                     "/Windows/System32/config/SYSTEM"],
                    capture_output=True, text=True, env=gf_env,
                )
                logger.info("  Disabled Windows Fast Startup (HiberbootEnabled=0)")
        except Exception as e:
            logger.debug(f"  Fast Startup disable attempt: {e}")

    def _inject_virtio_fallback(self, boot_disk, os_family):
        """Fallback VirtIO injection when virt-v2v fails."""
        if os_family == "windows":
            # Use offline driver injection (registry + sys files) for Windows
            from vmware2scw.converter.windows_virtio import inject_virtio_windows
            virtio_iso = self.config.conversion.virtio_win_iso
            if not virtio_iso or not Path(virtio_iso).exists():
                raise RuntimeError(
                    "virtio-win ISO is required for Windows VMs.\n"
                    "  wget -O /opt/virtio-win.iso "
                    "https://fedorapeople.org/groups/virt/virtio-win/direct-downloads/stable-virtio/virtio-win.iso\n"
                    "  Then in migration.yaml: conversion.virtio_win_iso: /opt/virtio-win.iso"
                )
            inject_virtio_windows(
                str(boot_disk),
                str(virtio_iso),
                work_dir=boot_disk.parent / "virtio-work",
            )
        else:
            from vmware2scw.converter.disk import VirtIOInjector
            injector = VirtIOInjector(
                virtio_win_iso=self.config.conversion.virtio_win_iso
            )
            injector.inject(str(boot_disk), os_family=os_family)

    def _ensure_rhsrvany(self):
        """Ensure rhsrvany.exe is installed for Windows virt-v2v conversions.

        On Ubuntu/Debian, virt-v2v requires rhsrvany.exe + pnp_wait.exe
        in /usr/share/virt-tools/ but these are not packaged. We extract
        them from the Fedora mingw32-srvany RPM.

        Ref: https://github.com/rwmjones/rhsrvany
        """
        import subprocess
        virt_tools = Path("/usr/share/virt-tools")
        rhsrvany = virt_tools / "rhsrvany.exe"

        if rhsrvany.exists():
            logger.info(f"rhsrvany.exe already present at {rhsrvany}")
            return

        logger.info("Installing rhsrvany.exe (required by virt-v2v for Windows)...")
        virt_tools.mkdir(parents=True, exist_ok=True)

        # Install rpm2cpio if needed
        subprocess.run(["apt-get", "install", "-y", "-qq", "rpm2cpio"], check=False, capture_output=True)

        rpm_url = "https://kojipkgs.fedoraproject.org//packages/mingw-srvany/1.1/4.fc38/noarch/mingw32-srvany-1.1-4.fc38.noarch.rpm"
        tmp_rpm = Path("/tmp/srvany.rpm")

        try:
            subprocess.run(["wget", "-q", "-O", str(tmp_rpm), rpm_url], check=True)
            # Extract exe files from RPM
            result = subprocess.run(
                f"cd /tmp && rpm2cpio {tmp_rpm} | cpio -idmv 2>&1",
                shell=True, capture_output=True, text=True,
            )
            # Find and copy the exe files
            import glob
            for exe in glob.glob("/tmp/usr/**/bin/*.exe", recursive=True):
                dest = virt_tools / Path(exe).name
                subprocess.run(["cp", exe, str(dest)], check=True)
                logger.info(f"  Installed {dest}")
        except Exception as e:
            logger.warning(f"Failed to install rhsrvany.exe: {e}")
            logger.warning("Windows virt-v2v conversion may fail. Install manually:")
            logger.warning(f"  wget -O /tmp/srvany.rpm {rpm_url}")
            logger.warning("  cd /tmp && rpm2cpio srvany.rpm | cpio -idmv")
            logger.warning("  cp /tmp/usr/*/bin/*.exe /usr/share/virt-tools/")
        finally:
            tmp_rpm.unlink(missing_ok=True)

    def _stage_convert(self, plan: VMMigrationPlan, state: MigrationState) -> None:
        """Convert VMDK disks to qcow2 format."""
        from vmware2scw.converter.disk import DiskConverter
        from vmware2scw.scaleway.mapping import ResourceMapper

        converter = DiskConverter()
        qcow2_paths = []

        # Determine OS family for compression decision
        mapper = ResourceMapper()
        vm_info_dict = state.artifacts.get("vm_info", {})
        guest_os = vm_info_dict.get("guest_os", "otherLinux64Guest")
        os_family, _ = mapper.get_os_family(guest_os)

        # Windows: do NOT compress — qemu-nbd has I/O errors on compressed qcow2
        # The image will be compressed later before upload if needed.
        compress = self.config.conversion.compress_qcow2
        if os_family == "windows":
            compress = False
            logger.info("Windows VM: disabling qcow2 compression (required for ntfsfix/qemu-nbd)")

        for vmdk_path in state.artifacts.get("vmdk_paths", []):
            vmdk = Path(vmdk_path)
            qcow2_path = vmdk.with_suffix(".qcow2")

            # Skip if already converted and valid
            if qcow2_path.exists() and converter.check(qcow2_path):
                logger.info(f"Skipping conversion (already exists): {qcow2_path.name}")
                qcow2_paths.append(str(qcow2_path))
                continue

            converter.convert(
                vmdk,
                qcow2_path,
                compress=compress,
            )
            qcow2_paths.append(str(qcow2_path))

        state.artifacts["qcow2_paths"] = qcow2_paths

        # Free disk space: delete VMDK source files after successful conversion
        for vmdk_path in state.artifacts.get("vmdk_paths", []):
            vmdk = Path(vmdk_path)
            if vmdk.exists():
                size_mb = vmdk.stat().st_size / (1024**2)
                vmdk.unlink()
                logger.info(f"Deleted source VMDK: {vmdk.name} ({size_mb:.0f} MB freed)")

    def _stage_fix_bootloader(self, plan: VMMigrationPlan, state: MigrationState) -> None:
        """Fix bootloader for KVM: fstab device names, GRUB config, initramfs.

        VMware uses LSI Logic / PVSCSI controllers → /dev/sd* devices.
        KVM with VirtIO uses /dev/vd* devices.

        If fstab or GRUB reference /dev/sda, the VM won't boot.
        Modern systems use UUID/LABEL which is safe, but we fix both.
        """
        from vmware2scw.scaleway.mapping import ResourceMapper
        from vmware2scw.utils.subprocess import run_command

        mapper = ResourceMapper()
        vm_info_dict = state.artifacts.get("vm_info", {})
        guest_os = vm_info_dict.get("guest_os", "")
        os_family, _ = mapper.get_os_family(guest_os)

        qcow2_paths = state.artifacts.get("qcow2_paths", [])
        if not qcow2_paths:
            return

        boot_disk = qcow2_paths[0]

        if os_family == "windows":
            # EMS, RDP, and DHCP are ALL configured by ensure_all_virtio_drivers
            # (via the SetupPhase script vmware2scw-setup.cmd in inject_virtio).
            # The NTFS is dirty after QEMU Phase 2 — do NOT try to write.
            logger.info("Windows: EMS/RDP/DHCP already configured by inject_virtio — skipping")
            return

        logger.info("Fixing bootloader for KVM compatibility...")

        # All fixes in a single virt-customize call to avoid multiple guest inspections
        commands = [
            # 1. Fix /etc/fstab: replace /dev/sd* with /dev/vd* (only if not UUID)
            "--run-command",
            "if [ -f /etc/fstab ]; then "
            "  cp /etc/fstab /etc/fstab.vmware2scw.bak; "
            "  sed -i 's|/dev/sda|/dev/vda|g; s|/dev/sdb|/dev/vdb|g; s|/dev/sdc|/dev/vdc|g' /etc/fstab; "
            "fi",

            # 2. Fix GRUB config: replace sd* references with vd*
            "--run-command",
            "if [ -f /etc/default/grub ]; then "
            "  cp /etc/default/grub /etc/default/grub.vmware2scw.bak; "
            "  sed -i 's|/dev/sda|/dev/vda|g' /etc/default/grub; "
            "fi",

            # 3. Fix GRUB device map
            "--run-command",
            "if [ -f /boot/grub/device.map ]; then "
            "  sed -i 's|/dev/sda|/dev/vda|g' /boot/grub/device.map; "
            "fi",

            # 4. Regenerate GRUB config
            "--run-command",
            "if command -v grub-mkconfig >/dev/null 2>&1; then "
            "  grub-mkconfig -o /boot/grub/grub.cfg 2>/dev/null || true; "
            "elif command -v grub2-mkconfig >/dev/null 2>&1; then "
            "  grub2-mkconfig -o /boot/grub2/grub.cfg 2>/dev/null || true; "
            "fi",

            # 5. Ensure VirtIO modules are loaded at boot
            "--run-command",
            "if [ -d /etc/initramfs-tools ]; then "
            "  for mod in virtio_blk virtio_scsi virtio_net virtio_pci; do "
            "    grep -q $mod /etc/initramfs-tools/modules 2>/dev/null || echo $mod >> /etc/initramfs-tools/modules; "
            "  done; "
            "  update-initramfs -u 2>/dev/null || true; "
            "elif command -v dracut >/dev/null 2>&1; then "
            "  dracut --force --add-drivers 'virtio_blk virtio_scsi virtio_net virtio_pci' 2>/dev/null || true; "
            "fi",

            # 6. Remove VMware SCSI driver references that interfere with VirtIO
            "--run-command",
            "rm -f /etc/modprobe.d/*vmw* 2>/dev/null || true; "
            "rm -f /etc/modprobe.d/*vmware* 2>/dev/null || true",

            # 7. Clean persistent net rules (interface names change)
            "--run-command",
            "rm -f /etc/udev/rules.d/70-persistent-net.rules 2>/dev/null || true; "
            "rm -f /etc/udev/rules.d/75-persistent-net-generator.rules 2>/dev/null || true",

            # 8. Enable DHCP on first interface (Scaleway provides IP via DHCP)
            "--run-command",
            "if [ -d /etc/netplan ]; then "
            "  cat > /etc/netplan/50-cloud-init.yaml << 'NETPLAN'\n"
            "network:\n"
            "  version: 2\n"
            "  ethernets:\n"
            "    ens2:\n"
            "      dhcp4: true\n"
            "    eth0:\n"
            "      dhcp4: true\n"
            "NETPLAN\n"
            "elif [ -d /etc/sysconfig/network-scripts ]; then "
            "  cat > /etc/sysconfig/network-scripts/ifcfg-eth0 << 'IFCFG'\n"
            "DEVICE=eth0\n"
            "ONBOOT=yes\n"
            "BOOTPROTO=dhcp\n"
            "IFCFG\n"
            "fi",
        ]

        cmd = ["virt-customize", "-a", str(boot_disk)] + commands
        run_command(cmd, env={"LIBGUESTFS_BACKEND": "direct"}, check=False)
        logger.info("Bootloader and network configuration fixed for KVM")

    def _stage_ensure_uefi(self, plan: VMMigrationPlan, state: MigrationState) -> None:
        """Ensure disk is UEFI-bootable. Scaleway uses UEFI firmware.

        If the source VM is BIOS/MBR (common for VMware), we must:
        - Convert MBR→GPT
        - Create an EFI System Partition (ESP)
        - Install GRUB EFI bootloader

        This is normally handled by virt-v2v, but falls back to manual
        conversion when virt-v2v fails (e.g. Ubuntu 24.04 kernel bug).
        """
        from vmware2scw.converter.bios2uefi import detect_boot_type, convert_bios_to_uefi
        from vmware2scw.scaleway.mapping import ResourceMapper

        mapper = ResourceMapper()
        vm_info_dict = state.artifacts.get("vm_info", {})
        guest_os = vm_info_dict.get("guest_os", "")
        firmware = vm_info_dict.get("firmware", "bios")
        os_family, _ = mapper.get_os_family(guest_os)

        qcow2_paths = state.artifacts.get("qcow2_paths", [])
        if not qcow2_paths:
            return

        boot_disk = qcow2_paths[0]

        # If virt-v2v succeeded (inject_virtio didn't fall back), disk should already be OK
        # Check anyway to be sure
        boot_type = detect_boot_type(boot_disk)
        logger.info(f"Boot type detection: firmware={firmware}, disk={boot_type}")

        if boot_type == "uefi":
            logger.info("Disk already UEFI-bootable — skipping conversion")
            return

        if os_family == "windows":
            logger.warning(
                "Windows BIOS→UEFI requires virt-v2v + virtio-win ISO. "
                "Manual conversion not supported. Consider using a RHEL/CentOS "
                "conversion host where virt-v2v works correctly."
            )
            return

        logger.info("Disk is BIOS — converting to UEFI for Scaleway compatibility")
        converted = convert_bios_to_uefi(boot_disk, os_family=os_family)
        if converted:
            logger.info("BIOS → UEFI conversion successful")
        else:
            logger.warning("BIOS → UEFI conversion was not performed")

    def _stage_fix_network(self, plan: VMMigrationPlan, state: MigrationState) -> None:
        """Network adaptation for Scaleway.

        Linux: handled in fix_bootloader stage.
        Windows: DHCP already forced in inject_virtio (ensure_all_virtio_drivers).
                 Add firstboot script as belt-and-suspenders.
        """
        from vmware2scw.scaleway.mapping import ResourceMapper
        from vmware2scw.utils.subprocess import run_command

        mapper = ResourceMapper()
        vm_info_dict = state.artifacts.get("vm_info", {})
        guest_os = vm_info_dict.get("guest_os", "")
        os_family, _ = mapper.get_os_family(guest_os)

        if os_family != "windows":
            logger.info("Linux network adaptation already handled in fix_bootloader stage")
            return

        # Windows: DHCP already configured by ensure_all_virtio_drivers in inject_virtio.
        # The NTFS is dirty after QEMU Phase 2 — do NOT try to write via virt-customize.
        logger.info("Windows network: DHCP already configured by inject_virtio — skipping")

    def _stage_upload_s3(self, plan: VMMigrationPlan, state: MigrationState) -> None:
        """Upload qcow2 images to Scaleway Object Storage."""
        from vmware2scw.scaleway.s3 import ScalewayS3

        scw_secret = self.config.scaleway.secret_key
        s3 = ScalewayS3(
            region=self.config.scaleway.s3_region,
            access_key=self.config.scaleway.access_key or "",
            secret_key=scw_secret.get_secret_value() if scw_secret else "",
        )

        bucket = self.config.scaleway.s3_bucket
        s3.create_bucket_if_not_exists(bucket)

        s3_keys = []
        for qcow2_path in state.artifacts.get("qcow2_paths", []):
            p = Path(qcow2_path)
            key = f"migrations/{state.migration_id}/{p.name}"

            # Skip if already uploaded with same size
            if s3.check_object_exists(bucket, key):
                remote_size = s3.get_object_size(bucket, key)
                local_size = p.stat().st_size
                if remote_size == local_size:
                    logger.info(f"Skipping upload (already exists): {key}")
                    s3_keys.append(key)
                    continue

            s3.upload_image(qcow2_path, bucket, key)
            s3_keys.append(key)

        state.artifacts["s3_keys"] = s3_keys
        state.artifacts["s3_bucket"] = bucket

    def _stage_import_scw(self, plan: VMMigrationPlan, state: MigrationState) -> None:
        """Import qcow2 image into Scaleway: create snapshot → image.

        Confidence: 80 — API workflow is documented but import from S3
        has specific requirements.
        """
        from vmware2scw.scaleway.instance import ScalewayInstanceAPI

        api = ScalewayInstanceAPI(
            access_key=self.config.scaleway.access_key or "",
            secret_key=(self.config.scaleway.secret_key.get_secret_value()
                        if self.config.scaleway.secret_key else ""),
            project_id=self.config.scaleway.project_id,
        )

        zone = plan.zone
        bucket = state.artifacts["s3_bucket"]
        s3_keys = state.artifacts.get("s3_keys", [])

        if not s3_keys:
            raise RuntimeError("No S3 keys found — upload stage may have failed")

        # Import the boot disk (first disk)
        boot_key = s3_keys[0]
        snapshot_name = f"vmware2scw-{plan.vm_name}-{state.migration_id}"

        logger.info(f"Creating Scaleway snapshot from s3://{bucket}/{boot_key}")
        snapshot = api.create_snapshot_from_s3(
            zone=zone,
            name=snapshot_name,
            bucket=bucket,
            key=boot_key,
        )
        snapshot_id = snapshot["id"]
        state.artifacts["scaleway_snapshot_id"] = snapshot_id

        logger.info(f"Waiting for snapshot {snapshot_id}...")
        api.wait_for_snapshot(zone, snapshot_id)

        # Create image from snapshot
        image_name = f"migrated-{plan.vm_name}"
        logger.info(f"Creating Scaleway image '{image_name}'")
        image = api.create_image(zone, image_name, snapshot_id)
        state.artifacts["scaleway_image_id"] = image["id"]

        logger.info(f"Image created: {image['id']}")

    def _stage_verify(self, plan: VMMigrationPlan, state: MigrationState) -> None:
        """Post-migration verification.

        Confidence: 75 — SPÉCULATIF. Basic checks only.
        """
        image_id = state.artifacts.get("scaleway_image_id")
        if image_id:
            logger.info(f"✅ Scaleway image created: {image_id}")
        else:
            logger.warning("⚠️  No Scaleway image ID found — verify manually")

        # TODO: Optionally boot a test instance and check connectivity

    def _stage_cleanup(self, plan: VMMigrationPlan, state: MigrationState) -> None:
        """Clean up all temporary resources to free disk space."""
        import shutil

        # 1. Clean local work directory (VMDK + qcow2 intermediate files)
        work_dir = self.config.conversion.work_dir / state.migration_id
        if work_dir.exists():
            size_gb = sum(f.stat().st_size for f in work_dir.rglob("*") if f.is_file()) / (1024**3)
            logger.info(f"Cleaning work directory: {work_dir} ({size_gb:.1f} GB)")
            shutil.rmtree(work_dir, ignore_errors=True)

        # 2. Clean VMware snapshot
        snap_name = state.artifacts.get("snapshot_name")
        if snap_name:
            try:
                from vmware2scw.vmware.client import VSphereClient
                from vmware2scw.vmware.snapshot import SnapshotManager

                client = VSphereClient()
                pw = self.config.vmware.password.get_secret_value() if self.config.vmware.password else ""
                client.connect(
                    self.config.vmware.vcenter,
                    self.config.vmware.username,
                    pw,
                    insecure=self.config.vmware.insecure,
                )
                snap_mgr = SnapshotManager(client)
                snap_mgr.delete_migration_snapshot(plan.vm_name, snap_name)
                client.disconnect()
                logger.info(f"Deleted VMware snapshot: {snap_name}")
            except Exception as e:
                logger.warning(f"Failed to clean VMware snapshot: {e}")

        # 3. Clean S3 transit files (safe now — image is created)
        image_id = state.artifacts.get("scaleway_image_id")
        s3_keys = state.artifacts.get("s3_keys", [])
        bucket = state.artifacts.get("s3_bucket")
        if image_id and s3_keys and bucket:
            try:
                from vmware2scw.scaleway.storage import ScalewayS3Client
                s3 = ScalewayS3Client(
                    access_key=self.config.scaleway.access_key,
                    secret_key=self.config.scaleway.secret_key.get_secret_value() if self.config.scaleway.secret_key else "",
                    region=self.config.scaleway.region,
                )
                for key in s3_keys:
                    try:
                        s3.client.delete_object(Bucket=bucket, Key=key)
                        logger.info(f"Deleted S3 transit: s3://{bucket}/{key}")
                    except Exception as e2:
                        logger.warning(f"Failed to delete {key}: {e2}")
            except Exception as e:
                logger.warning(f"S3 cleanup failed: {e}")
        else:
            logger.info("S3 transit files retained (image not confirmed or no keys)")

        logger.info("Cleanup complete.")
