"""VMware VM inventory collection.

Collects VM information from vCenter for use in migration planning:
  - VM name, CPU, memory, disk layout
  - Guest OS detection
  - Firmware type (BIOS/EFI)
  - ESXi host, cluster, datacenter, folder
  - Power state and VMware Tools status

NOTE: This is a stub/interface. Integrate your existing pyVmomi inventory
code here. The CLI and pipeline call these methods.
"""

from __future__ import annotations

import logging
from typing import Optional

from pydantic import BaseModel, Field

from vmware2scw.vmware.client import VSphereClient

logger = logging.getLogger(__name__)


class DiskInfo(BaseModel):
    """Information about a single VM disk."""
    name: str = ""
    size_gb: float = 0.0
    thin_provisioned: bool = True
    datastore: str = ""
    file_path: str = ""  # [datastore] path/to/disk.vmdk


class VMInfo(BaseModel):
    """Complete information about a VMware VM."""
    name: str
    moref: str = ""                    # Managed Object Reference
    power_state: str = ""              # "poweredOn", "poweredOff", "suspended"
    cpu: int = 0
    memory_mb: int = 0
    guest_os: str = ""                 # VMware guestId (e.g., "ubuntu64Guest")
    guest_os_full: str = ""            # Full guest OS name from VMware Tools
    firmware: str = "bios"             # "bios" or "efi"
    disks: list[DiskInfo] = Field(default_factory=list)
    total_disk_gb: float = 0.0
    networks: list[str] = Field(default_factory=list)
    host: str = ""                     # ESXi host
    cluster: str = ""                  # vCenter cluster
    datacenter: str = ""               # vCenter datacenter
    folder: str = ""                   # VM folder path
    resource_pool: str = ""
    tags: list[str] = Field(default_factory=list)
    tools_status: str = ""             # "toolsOk", "toolsNotInstalled", etc.
    tools_version: str = ""
    annotation: str = ""               # VM notes
    uuid: str = ""                     # BIOS UUID
    instance_uuid: str = ""            # vCenter instance UUID


class VMInventory:
    """Collect VM inventory from vCenter.

    Usage:
        client = VSphereClient()
        client.connect(...)
        inv = VMInventory(client)
        vms = inv.list_all_vms()
    """

    def __init__(self, client: VSphereClient):
        self.client = client

    def list_all_vms(self) -> list[VMInfo]:
        """List all VMs in the connected vCenter.

        TODO: Integrate your existing pyVmomi container view code.
        This should iterate all VMs and collect VMInfo for each.
        """
        logger.info("Collecting VM inventory...")
        # TODO: Replace with actual pyVmomi inventory collection
        # content = self.client.content
        # container = content.viewManager.CreateContainerView(
        #     content.rootFolder, [vim.VirtualMachine], True)
        # for vm in container.view:
        #     ...
        return []

    def get_vm_info(self, vm_name: str) -> VMInfo | None:
        """Get detailed info for a specific VM by name.

        TODO: Integrate your existing VM lookup code.
        """
        logger.info(f"Looking up VM: {vm_name}")
        # TODO: Replace with actual lookup
        return None

    def get_vm_by_moref(self, moref: str) -> VMInfo | None:
        """Get VM info by Managed Object Reference."""
        # TODO: Replace with actual moref lookup
        return None
