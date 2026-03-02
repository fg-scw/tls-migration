"""Pre-migration compatibility validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from vmware2scw.scaleway.mapping import INSTANCE_TYPES, ResourceMapper
from vmware2scw.utils.logging import get_logger

if TYPE_CHECKING:
    from vmware2scw.vmware.inventory import VMInfo

logger = get_logger(__name__)


@dataclass
class ValidationCheck:
    """Result of a single validation check."""
    name: str
    passed: bool
    message: str
    blocking: bool = True  # If False, it's a warning not an error


@dataclass
class ValidationReport:
    """Complete validation report for a VM migration."""
    vm_name: str
    target_type: str
    checks: list[ValidationCheck] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks if c.blocking)

    @property
    def warnings(self) -> list[ValidationCheck]:
        return [c for c in self.checks if not c.blocking and not c.passed]


class MigrationValidator:
    """Validates VM compatibility with Scaleway target instance type.

    Runs a series of checks to ensure the VM can be successfully migrated.
    Each check is independent and produces a clear pass/fail with message.

    Confidence: 80 — Covers the major incompatibilities. Edge cases
    (specific driver issues, kernel modules) may not be caught.
    """

    def __init__(self):
        self.mapper = ResourceMapper()

    def validate(self, vm_info: "VMInfo", target_type: str) -> ValidationReport:
        """Run all validation checks against a VM.

        Args:
            vm_info: VMware VM information
            target_type: Target Scaleway instance type (e.g. "PRO2-S")

        Returns:
            ValidationReport with all check results
        """
        report = ValidationReport(vm_name=vm_info.name, target_type=target_type)

        checks = [
            self._check_target_type_exists,
            self._check_cpu_fit,
            self._check_ram_fit,
            self._check_disk_size,
            self._check_disk_count,
            self._check_os_supported,
            self._check_firmware,
            self._check_no_rdm,
            self._check_no_snapshots_complex,
            self._check_tools_status,
            self._check_power_state,
        ]

        for check_fn in checks:
            result = check_fn(vm_info, target_type)
            report.checks.append(result)

        return report

    def _check_target_type_exists(self, vm: "VMInfo", target: str) -> ValidationCheck:
        """Verify the target instance type exists in our catalog."""
        if target in INSTANCE_TYPES:
            spec = INSTANCE_TYPES[target]
            return ValidationCheck(
                name="Instance type",
                passed=True,
                message=f"{target} found ({spec.vcpus} vCPU, {spec.ram_gb}GB RAM)",
            )
        return ValidationCheck(
            name="Instance type",
            passed=False,
            message=f"Unknown instance type '{target}'. Available: {', '.join(sorted(INSTANCE_TYPES.keys())[:10])}...",
        )

    def _check_cpu_fit(self, vm: "VMInfo", target: str) -> ValidationCheck:
        """Check if VM vCPUs fit in the target instance type."""
        spec = INSTANCE_TYPES.get(target)
        if not spec:
            return ValidationCheck("CPU fit", False, "Cannot check — unknown instance type")

        if vm.cpu <= spec.vcpus:
            return ValidationCheck("CPU fit", True, f"VM {vm.cpu} vCPU ≤ {target} {spec.vcpus} vCPU")
        return ValidationCheck(
            "CPU fit", False,
            f"VM needs {vm.cpu} vCPUs but {target} only has {spec.vcpus}. "
            f"Consider a larger type.",
        )

    def _check_ram_fit(self, vm: "VMInfo", target: str) -> ValidationCheck:
        """Check if VM RAM fits in the target instance type."""
        spec = INSTANCE_TYPES.get(target)
        if not spec:
            return ValidationCheck("RAM fit", False, "Cannot check — unknown instance type")

        vm_ram_gb = vm.memory_mb / 1024
        if vm_ram_gb <= spec.ram_gb:
            return ValidationCheck("RAM fit", True, f"VM {vm_ram_gb:.1f}GB ≤ {target} {spec.ram_gb}GB")
        return ValidationCheck(
            "RAM fit", False,
            f"VM needs {vm_ram_gb:.1f}GB RAM but {target} only has {spec.ram_gb}GB.",
        )

    def _check_disk_size(self, vm: "VMInfo", target: str) -> ValidationCheck:
        """Check if VM disks fit within storage limits."""
        spec = INSTANCE_TYPES.get(target)
        if not spec:
            return ValidationCheck("Disk size", False, "Cannot check — unknown instance type")

        total_gb = vm.total_disk_gb

        if spec.block_storage:
            # Block storage max per volume: 10TB
            max_single = max((d.size_gb for d in vm.disks), default=0)
            if max_single > 10000:
                return ValidationCheck(
                    "Disk size", False,
                    f"Largest disk ({max_single:.0f}GB) exceeds block storage max (10TB)",
                )
            return ValidationCheck("Disk size", True, f"Total {total_gb:.0f}GB fits in block storage")
        else:
            if total_gb <= spec.local_storage_gb:
                return ValidationCheck(
                    "Disk size", True,
                    f"Total {total_gb:.0f}GB ≤ local storage {spec.local_storage_gb}GB",
                )
            return ValidationCheck(
                "Disk size", False,
                f"Total {total_gb:.0f}GB exceeds {target} local storage ({spec.local_storage_gb}GB)",
            )

    def _check_disk_count(self, vm: "VMInfo", target: str) -> ValidationCheck:
        """Check if VM disk count is within limits."""
        spec = INSTANCE_TYPES.get(target)
        if not spec:
            return ValidationCheck("Disk count", False, "Cannot check — unknown instance type")

        if len(vm.disks) <= spec.max_volumes:
            return ValidationCheck("Disk count", True, f"{len(vm.disks)} disks ≤ {spec.max_volumes} max")
        return ValidationCheck(
            "Disk count", False,
            f"VM has {len(vm.disks)} disks but {target} supports max {spec.max_volumes}",
        )

    def _check_os_supported(self, vm: "VMInfo", target: str) -> ValidationCheck:
        """Check if the OS is known and supported for migration."""
        os_family, os_desc = self.mapper.get_os_family(vm.guest_os)

        if os_family == "unknown":
            return ValidationCheck(
                "OS support", False,
                f"Unknown OS type '{vm.guest_os}'. Manual verification needed.",
                blocking=False,
            )

        return ValidationCheck("OS support", True, f"{os_desc} ({os_family})")

    def _check_firmware(self, vm: "VMInfo", target: str) -> ValidationCheck:
        """Check firmware compatibility (BIOS vs UEFI)."""
        if vm.firmware == "efi":
            return ValidationCheck(
                "Firmware", True,
                "UEFI detected. Scaleway instances support UEFI boot.",
                blocking=False,
            )
        return ValidationCheck("Firmware", True, f"BIOS firmware — compatible")

    def _check_no_rdm(self, vm: "VMInfo", target: str) -> ValidationCheck:
        """Check for Raw Device Mapping disks (not migratable)."""
        # RDM disks would show as a different backing type
        for disk in vm.disks:
            if "rdm" in disk.path.lower() or "raw" in disk.controller_type.lower():
                return ValidationCheck(
                    "RDM disks", False,
                    f"Disk '{disk.name}' appears to be RDM — not supported for migration",
                )
        return ValidationCheck("RDM disks", True, "No RDM disks detected")

    def _check_no_snapshots_complex(self, vm: "VMInfo", target: str) -> ValidationCheck:
        """Warn about existing snapshots that may complicate export."""
        if len(vm.snapshots) > 3:
            return ValidationCheck(
                "Snapshots", False,
                f"VM has {len(vm.snapshots)} snapshots. Consolidate before migration for best results.",
                blocking=False,
            )
        if vm.snapshots:
            return ValidationCheck(
                "Snapshots", True,
                f"VM has {len(vm.snapshots)} snapshot(s) — acceptable",
            )
        return ValidationCheck("Snapshots", True, "No existing snapshots")

    def _check_tools_status(self, vm: "VMInfo", target: str) -> ValidationCheck:
        """Check VMware Tools status."""
        if vm.tools_status in ("toolsOk", "toolsOld"):
            return ValidationCheck(
                "VMware Tools", True,
                f"Tools status: {vm.tools_status} — will be cleaned during migration",
            )
        if vm.tools_status == "toolsNotInstalled":
            return ValidationCheck(
                "VMware Tools", True,
                "Tools not installed — no cleanup needed",
            )
        return ValidationCheck(
            "VMware Tools", True,
            f"Tools status: {vm.tools_status}",
            blocking=False,
        )

    def _check_power_state(self, vm: "VMInfo", target: str) -> ValidationCheck:
        """Inform about VM power state."""
        if "poweredOn" in vm.power_state:
            return ValidationCheck(
                "Power state", True,
                "VM is powered on — hot migration will create a snapshot before export",
                blocking=False,
            )
        return ValidationCheck(
            "Power state", True,
            f"VM is {vm.power_state}",
        )
