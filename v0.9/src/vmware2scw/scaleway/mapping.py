"""Scaleway instance type mapping and resource suggestions.

Catalogue updated from: https://www.scaleway.com/en/pricing/virtual-instances/
Last update: 2026-02-19
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from vmware2scw.utils.logging import get_logger

if TYPE_CHECKING:
    from vmware2scw.vmware.inventory import VMInfo

logger = get_logger(__name__)


@dataclass
class InstanceTypeSpec:
    """Specification of a Scaleway instance type."""
    name: str
    vcpus: int
    ram_gb: int
    bandwidth_mbps: int = 200
    block_storage: bool = True
    max_volumes: int = 16
    local_storage_gb: int = 0
    category: str = "general"
    windows: bool = False
    shared_vcpu: bool = False
    price_hour_eur: float = 0.0
    arch: str = "x86_64"


# ═══════════════════════════════════════════════════════════════════
# SCALEWAY INSTANCE TYPE CATALOG
# Source: https://www.scaleway.com/en/pricing/virtual-instances/
# ═══════════════════════════════════════════════════════════════════

INSTANCE_TYPES: dict[str, InstanceTypeSpec] = {

    # ── Development (shared vCPUs, local storage) ────────────────
    "STARDUST1-S":  InstanceTypeSpec("STARDUST1-S",  vcpus=1, ram_gb=1,  bandwidth_mbps=100,  local_storage_gb=10,  block_storage=False, max_volumes=1, category="development", shared_vcpu=True, price_hour_eur=0.00015),
    "DEV1-S":       InstanceTypeSpec("DEV1-S",       vcpus=2, ram_gb=2,  bandwidth_mbps=200,  local_storage_gb=20,  block_storage=False, max_volumes=1, category="development", shared_vcpu=True, price_hour_eur=0.0088),
    "DEV1-M":       InstanceTypeSpec("DEV1-M",       vcpus=3, ram_gb=4,  bandwidth_mbps=300,  local_storage_gb=40,  block_storage=False, max_volumes=1, category="development", shared_vcpu=True, price_hour_eur=0.0198),
    "DEV1-L":       InstanceTypeSpec("DEV1-L",       vcpus=4, ram_gb=8,  bandwidth_mbps=400,  local_storage_gb=80,  block_storage=False, max_volumes=1, category="development", shared_vcpu=True, price_hour_eur=0.042),
    "DEV1-XL":      InstanceTypeSpec("DEV1-XL",      vcpus=4, ram_gb=12, bandwidth_mbps=500,  local_storage_gb=120, block_storage=False, max_volumes=1, category="development", shared_vcpu=True, price_hour_eur=0.0638),

    # ── General Purpose — Shared vCPUs (PLAY2) ──────────────────
    "PLAY2-PICO":   InstanceTypeSpec("PLAY2-PICO",   vcpus=1, ram_gb=2,  bandwidth_mbps=100,  category="general", shared_vcpu=True, price_hour_eur=0.014),
    "PLAY2-NANO":   InstanceTypeSpec("PLAY2-NANO",   vcpus=2, ram_gb=4,  bandwidth_mbps=200,  category="general", shared_vcpu=True, price_hour_eur=0.027),
    "PLAY2-MICRO":  InstanceTypeSpec("PLAY2-MICRO",  vcpus=4, ram_gb=8,  bandwidth_mbps=400,  category="general", shared_vcpu=True, price_hour_eur=0.054),

    # ── General Purpose — Shared vCPUs (BASIC2-A) ───────────────
    "BASIC2-A2C-4G":   InstanceTypeSpec("BASIC2-A2C-4G",   vcpus=2,  ram_gb=4,   bandwidth_mbps=200,  category="general", shared_vcpu=True, price_hour_eur=0.023),
    "BASIC2-A4C-8G":   InstanceTypeSpec("BASIC2-A4C-8G",   vcpus=4,  ram_gb=8,   bandwidth_mbps=400,  category="general", shared_vcpu=True, price_hour_eur=0.0517),
    "BASIC2-A2C-8G":   InstanceTypeSpec("BASIC2-A2C-8G",   vcpus=2,  ram_gb=8,   bandwidth_mbps=200,  category="general", shared_vcpu=True, price_hour_eur=0.0345),
    "BASIC2-A4C-16G":  InstanceTypeSpec("BASIC2-A4C-16G",  vcpus=4,  ram_gb=16,  bandwidth_mbps=400,  category="general", shared_vcpu=True, price_hour_eur=0.0689),
    "BASIC2-A8C-16G":  InstanceTypeSpec("BASIC2-A8C-16G",  vcpus=8,  ram_gb=16,  bandwidth_mbps=800,  category="general", shared_vcpu=True, price_hour_eur=0.1034),
    "BASIC2-A8C-32G":  InstanceTypeSpec("BASIC2-A8C-32G",  vcpus=8,  ram_gb=32,  bandwidth_mbps=800,  category="general", shared_vcpu=True, price_hour_eur=0.1378),
    "BASIC2-A16C-32G": InstanceTypeSpec("BASIC2-A16C-32G", vcpus=16, ram_gb=32,  bandwidth_mbps=1600, category="general", shared_vcpu=True, price_hour_eur=0.2067),
    "BASIC2-A16C-64G": InstanceTypeSpec("BASIC2-A16C-64G", vcpus=16, ram_gb=64,  bandwidth_mbps=1600, category="general", shared_vcpu=True, price_hour_eur=0.2756),

    # ── General Purpose — Shared vCPUs (PRO2) ───────────────────
    "PRO2-XXS": InstanceTypeSpec("PRO2-XXS", vcpus=2,  ram_gb=8,   bandwidth_mbps=350,  category="general", shared_vcpu=True, price_hour_eur=0.055),
    "PRO2-XS":  InstanceTypeSpec("PRO2-XS",  vcpus=4,  ram_gb=16,  bandwidth_mbps=700,  category="general", shared_vcpu=True, price_hour_eur=0.11),
    "PRO2-S":   InstanceTypeSpec("PRO2-S",   vcpus=8,  ram_gb=32,  bandwidth_mbps=1500, category="general", shared_vcpu=True, price_hour_eur=0.219),
    "PRO2-M":   InstanceTypeSpec("PRO2-M",   vcpus=16, ram_gb=64,  bandwidth_mbps=3000, category="general", shared_vcpu=True, price_hour_eur=0.438),
    "PRO2-L":   InstanceTypeSpec("PRO2-L",   vcpus=32, ram_gb=128, bandwidth_mbps=6000, category="general", shared_vcpu=True, price_hour_eur=0.877),

    # ── General Purpose — Shared vCPUs (GP1, legacy local storage)
    "GP1-XS":  InstanceTypeSpec("GP1-XS",  vcpus=4,  ram_gb=16,  bandwidth_mbps=500,   local_storage_gb=150, block_storage=False, category="general", shared_vcpu=True, price_hour_eur=0.091),
    "GP1-S":   InstanceTypeSpec("GP1-S",   vcpus=8,  ram_gb=32,  bandwidth_mbps=800,   local_storage_gb=300, block_storage=False, category="general", shared_vcpu=True, price_hour_eur=0.187),
    "GP1-M":   InstanceTypeSpec("GP1-M",   vcpus=16, ram_gb=64,  bandwidth_mbps=1500,  local_storage_gb=600, block_storage=False, category="general", shared_vcpu=True, price_hour_eur=0.376),
    "GP1-L":   InstanceTypeSpec("GP1-L",   vcpus=32, ram_gb=128, bandwidth_mbps=5000,  local_storage_gb=600, block_storage=False, category="general", shared_vcpu=True, price_hour_eur=0.759),
    "GP1-XL":  InstanceTypeSpec("GP1-XL",  vcpus=48, ram_gb=256, bandwidth_mbps=10000, local_storage_gb=600, block_storage=False, category="general", shared_vcpu=True, price_hour_eur=1.641),

    # ── General Purpose — Dedicated vCPUs (POP2) ────────────────
    "POP2-2C-8G":     InstanceTypeSpec("POP2-2C-8G",     vcpus=2,  ram_gb=8,   bandwidth_mbps=400,   category="general_dedicated", price_hour_eur=0.0735),
    "POP2-4C-16G":    InstanceTypeSpec("POP2-4C-16G",    vcpus=4,  ram_gb=16,  bandwidth_mbps=800,   category="general_dedicated", price_hour_eur=0.147),
    "POP2-8C-32G":    InstanceTypeSpec("POP2-8C-32G",    vcpus=8,  ram_gb=32,  bandwidth_mbps=1600,  category="general_dedicated", price_hour_eur=0.29),
    "POP2-16C-64G":   InstanceTypeSpec("POP2-16C-64G",   vcpus=16, ram_gb=64,  bandwidth_mbps=3200,  category="general_dedicated", price_hour_eur=0.59),
    "POP2-32C-128G":  InstanceTypeSpec("POP2-32C-128G",  vcpus=32, ram_gb=128, bandwidth_mbps=6400,  category="general_dedicated", price_hour_eur=1.18),
    "POP2-48C-192G":  InstanceTypeSpec("POP2-48C-192G",  vcpus=48, ram_gb=192, bandwidth_mbps=9600,  category="general_dedicated", price_hour_eur=1.77),
    "POP2-64C-256G":  InstanceTypeSpec("POP2-64C-256G",  vcpus=64, ram_gb=256, bandwidth_mbps=12800, category="general_dedicated", price_hour_eur=2.35),

    # ── Windows — Dedicated vCPUs (POP2-WIN) ────────────────────
    "POP2-2C-8G-WIN":    InstanceTypeSpec("POP2-2C-8G-WIN",    vcpus=2,  ram_gb=8,   bandwidth_mbps=400,  category="windows", windows=True, price_hour_eur=0.1823),
    "POP2-4C-16G-WIN":   InstanceTypeSpec("POP2-4C-16G-WIN",   vcpus=4,  ram_gb=16,  bandwidth_mbps=800,  category="windows", windows=True, price_hour_eur=0.3637),
    "POP2-8C-32G-WIN":   InstanceTypeSpec("POP2-8C-32G-WIN",   vcpus=8,  ram_gb=32,  bandwidth_mbps=1600, category="windows", windows=True, price_hour_eur=0.7233),
    "POP2-16C-64G-WIN":  InstanceTypeSpec("POP2-16C-64G-WIN",  vcpus=16, ram_gb=64,  bandwidth_mbps=3200, category="windows", windows=True, price_hour_eur=1.4567),
    "POP2-32C-128G-WIN": InstanceTypeSpec("POP2-32C-128G-WIN", vcpus=32, ram_gb=128, bandwidth_mbps=6400, category="windows", windows=True, price_hour_eur=2.9133),

    # ── Compute Optimized — Dedicated vCPUs (POP2-HC) ───────────
    "POP2-HC-2C-4G":    InstanceTypeSpec("POP2-HC-2C-4G",    vcpus=2,  ram_gb=4,   bandwidth_mbps=400,   category="compute", price_hour_eur=0.0532),
    "POP2-HC-4C-8G":    InstanceTypeSpec("POP2-HC-4C-8G",    vcpus=4,  ram_gb=8,   bandwidth_mbps=800,   category="compute", price_hour_eur=0.1064),
    "POP2-HC-8C-16G":   InstanceTypeSpec("POP2-HC-8C-16G",   vcpus=8,  ram_gb=16,  bandwidth_mbps=1600,  category="compute", price_hour_eur=0.2128),
    "POP2-HC-16C-32G":  InstanceTypeSpec("POP2-HC-16C-32G",  vcpus=16, ram_gb=32,  bandwidth_mbps=3200,  category="compute", price_hour_eur=0.4256),
    "POP2-HC-32C-64G":  InstanceTypeSpec("POP2-HC-32C-64G",  vcpus=32, ram_gb=64,  bandwidth_mbps=6400,  category="compute", price_hour_eur=0.8512),
    "POP2-HC-48C-96G":  InstanceTypeSpec("POP2-HC-48C-96G",  vcpus=48, ram_gb=96,  bandwidth_mbps=9600,  category="compute", price_hour_eur=1.27),
    "POP2-HC-64C-128G": InstanceTypeSpec("POP2-HC-64C-128G", vcpus=64, ram_gb=128, bandwidth_mbps=12800, category="compute", price_hour_eur=1.7024),

    # ── Memory Optimized — Dedicated vCPUs (POP2-HM) ────────────
    "POP2-HM-2C-16G":   InstanceTypeSpec("POP2-HM-2C-16G",   vcpus=2,  ram_gb=16,  bandwidth_mbps=400,   category="memory", price_hour_eur=0.1),
    "POP2-HM-4C-32G":   InstanceTypeSpec("POP2-HM-4C-32G",   vcpus=4,  ram_gb=32,  bandwidth_mbps=800,   category="memory", price_hour_eur=0.2),
    "POP2-HM-8C-64G":   InstanceTypeSpec("POP2-HM-8C-64G",   vcpus=8,  ram_gb=64,  bandwidth_mbps=1600,  category="memory", price_hour_eur=0.4),
    "POP2-HM-16C-128G": InstanceTypeSpec("POP2-HM-16C-128G", vcpus=16, ram_gb=128, bandwidth_mbps=3200,  category="memory", price_hour_eur=0.8),
    "POP2-HM-32C-256G": InstanceTypeSpec("POP2-HM-32C-256G", vcpus=32, ram_gb=256, bandwidth_mbps=6400,  category="memory", price_hour_eur=1.6),
    "POP2-HM-48C-384G": InstanceTypeSpec("POP2-HM-48C-384G", vcpus=48, ram_gb=384, bandwidth_mbps=9600,  category="memory", price_hour_eur=2.4),
    "POP2-HM-64C-512G": InstanceTypeSpec("POP2-HM-64C-512G", vcpus=64, ram_gb=512, bandwidth_mbps=12800, category="memory", price_hour_eur=3.2),
}


# ═══════════════════════════════════════════════════════════════════
# OS MAPPING
# ═══════════════════════════════════════════════════════════════════

OS_FAMILY_MAP = {
    # Windows Server
    "windows9Server64Guest": ("windows", "Windows Server 2016+"),
    "windows9_64Guest": ("windows", "Windows 10+"),
    "windows2019srv_64Guest": ("windows", "Windows Server 2019"),
    "windows2019srvNext_64Guest": ("windows", "Windows Server 2022"),
    "windows2022srvNext_64Guest": ("windows", "Windows Server 2025"),
    # Debian/Ubuntu
    "ubuntu64Guest": ("linux", "Ubuntu"),
    "ubuntuGuest": ("linux", "Ubuntu"),
    "debian10_64Guest": ("linux", "Debian 10"),
    "debian11_64Guest": ("linux", "Debian 11"),
    "debian12_64Guest": ("linux", "Debian 12"),
    # RHEL family
    "rhel8_64Guest": ("linux", "RHEL 8"),
    "rhel9_64Guest": ("linux", "RHEL 9"),
    "centos8_64Guest": ("linux", "CentOS 8"),
    "centos9_64Guest": ("linux", "CentOS 9"),
    "rockylinux_64Guest": ("linux", "Rocky Linux"),
    "almalinux_64Guest": ("linux", "AlmaLinux"),
    # Other
    "sles15_64Guest": ("linux", "SLES 15"),
    "freebsd64Guest": ("linux", "FreeBSD"),
    "other4xLinux64Guest": ("linux", "Linux (generic)"),
    "otherLinux64Guest": ("linux", "Linux (generic)"),
    "other3xLinux64Guest": ("linux", "Linux (generic)"),
    "otherGuest64": ("unknown", "Unknown 64-bit"),
}


@dataclass
class InstanceTypeSuggestion:
    """A suggested Scaleway instance type for a VM."""
    instance_type: str
    vcpus: int
    ram_gb: int
    bandwidth_mbps: int = 0
    fit_score: float = 0.0
    notes: str = ""
    category: str = ""
    windows: bool = False
    price_hour_eur: float = 0.0
    price_month_eur: float = 0.0


class ResourceMapper:
    """Maps VMware VM resources to Scaleway instance types.

    For Windows VMs, automatically filters to POP2-*-WIN types.
    """

    def suggest_instance_type(
        self,
        vm_info: "VMInfo",
        exclude_dev: bool = True,
        prefer_dedicated: bool = True,
    ) -> list[InstanceTypeSuggestion]:
        """Suggest the best Scaleway instance types for a VM."""
        required_cpu = vm_info.cpu
        required_ram_gb = vm_info.memory_mb / 1024
        num_disks = len(vm_info.disks)

        os_family, _ = self.get_os_family(vm_info.guest_os)
        is_windows = os_family == "windows"

        suggestions = []

        for name, spec in INSTANCE_TYPES.items():
            if exclude_dev and spec.category == "development":
                continue
            # Windows VMs → only Windows types
            if is_windows and not spec.windows:
                continue
            # Non-Windows → exclude Windows types
            if not is_windows and spec.windows:
                continue
            if spec.vcpus < required_cpu:
                continue
            if spec.ram_gb < required_ram_gb:
                continue
            if num_disks > spec.max_volumes:
                continue
            if not spec.block_storage:
                if spec.local_storage_gb < vm_info.total_disk_gb:
                    continue

            cpu_ratio = required_cpu / spec.vcpus
            ram_ratio = required_ram_gb / spec.ram_gb
            fit_score = (cpu_ratio + ram_ratio) / 2
            if prefer_dedicated and not spec.shared_vcpu:
                fit_score *= 1.05
            fit_score = min(fit_score, 1.0)

            notes_parts = []
            if spec.windows:
                notes_parts.append("Windows license included")
            notes_parts.append("Dedicated vCPUs" if not spec.shared_vcpu else "Shared vCPUs")
            if spec.block_storage:
                notes_parts.append("Block storage (SBS)")
            else:
                notes_parts.append(f"Local SSD ({spec.local_storage_gb}GB)")

            suggestions.append(InstanceTypeSuggestion(
                instance_type=name,
                vcpus=spec.vcpus,
                ram_gb=spec.ram_gb,
                bandwidth_mbps=spec.bandwidth_mbps,
                fit_score=fit_score,
                notes="; ".join(notes_parts),
                category=spec.category,
                windows=spec.windows,
                price_hour_eur=spec.price_hour_eur,
                price_month_eur=round(spec.price_hour_eur * 730, 2),
            ))

        suggestions.sort(key=lambda s: s.fit_score, reverse=True)
        return suggestions[:5]

    def get_os_family(self, guest_os_id: str) -> tuple[str, str]:
        """Map VMware guest OS ID to OS family."""
        if guest_os_id in OS_FAMILY_MAP:
            return OS_FAMILY_MAP[guest_os_id]
        lower = guest_os_id.lower()
        if "win" in lower:
            return ("windows", "Windows (detected)")
        if any(x in lower for x in ["linux", "ubuntu", "debian", "rhel", "centos", "rocky", "alma", "suse", "fedora"]):
            return ("linux", "Linux (detected)")
        return ("unknown", f"Unknown ({guest_os_id})")
