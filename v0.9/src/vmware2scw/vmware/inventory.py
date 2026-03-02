"""VMware VM inventory collection and data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pyVmomi import vim

from vmware2scw.utils.logging import get_logger
from vmware2scw.vmware.client import VSphereClient

logger = get_logger(__name__)


@dataclass
class DiskInfo:
    """Information about a VM disk."""
    name: str
    size_gb: float
    thin_provisioned: bool
    datastore: str
    path: str                # e.g. "[datastore1] vm/vm.vmdk"
    controller_type: str     # "scsi", "nvme", "ide"
    key: int = 0             # vSphere device key


@dataclass
class NICInfo:
    """Information about a VM network adapter."""
    mac_address: str
    network: str
    adapter_type: str        # "vmxnet3", "e1000", "e1000e"
    connected: bool
    ip_addresses: list[str] = field(default_factory=list)


@dataclass
class VMInfo:
    """Complete information about a VMware virtual machine."""
    name: str
    uuid: str
    cpu: int
    memory_mb: int
    power_state: str
    guest_os: str              # VMware guest OS ID (e.g. "ubuntu64Guest")
    guest_os_full: Optional[str]   # Full OS name if tools report it
    firmware: str              # "bios" | "efi"
    disks: list[DiskInfo] = field(default_factory=list)
    nics: list[NICInfo] = field(default_factory=list)
    snapshots: list[str] = field(default_factory=list)
    tools_status: str = "unknown"
    tools_version: str = ""
    host: str = ""
    datacenter: str = ""
    cluster: str = ""
    annotation: str = ""

    @property
    def total_disk_gb(self) -> float:
        return sum(d.size_gb for d in self.disks)

    @property
    def is_windows(self) -> bool:
        return "win" in self.guest_os.lower()

    @property
    def is_linux(self) -> bool:
        return not self.is_windows

    @property
    def is_uefi(self) -> bool:
        return self.firmware == "efi"

    def model_dump(self) -> dict:
        """Serialize to dict for JSON output."""
        return {
            "name": self.name,
            "uuid": self.uuid,
            "cpu": self.cpu,
            "memory_mb": self.memory_mb,
            "power_state": self.power_state,
            "guest_os": self.guest_os,
            "guest_os_full": self.guest_os_full,
            "firmware": self.firmware,
            "total_disk_gb": self.total_disk_gb,
            "disks": [{"name": d.name, "size_gb": d.size_gb, "thin": d.thin_provisioned,
                        "datastore": d.datastore, "controller": d.controller_type} for d in self.disks],
            "nics": [{"mac": n.mac_address, "network": n.network, "type": n.adapter_type,
                       "connected": n.connected, "ips": n.ip_addresses} for n in self.nics],
            "tools_status": self.tools_status,
            "firmware": self.firmware,
            "host": self.host,
            "datacenter": self.datacenter,
            "cluster": self.cluster,
        }


class VMInventory:
    """Collects VM inventory from a vCenter instance.

    Uses PropertyCollector for efficient batch retrieval of VM properties
    rather than querying each VM individually.
    """

    def __init__(self, client: VSphereClient):
        self.client = client

    def list_all_vms(self) -> list[VMInfo]:
        """Retrieve information about all VMs in the vCenter."""
        container = self.client.get_container_view([vim.VirtualMachine])
        vms = []

        for vm_obj in container.view:
            try:
                vm_info = self._extract_vm_info(vm_obj)
                vms.append(vm_info)
            except Exception as e:
                logger.warning(f"Error collecting info for VM: {e}")

        container.Destroy()
        logger.info(f"Collected inventory for {len(vms)} VMs")
        return vms

    def get_vm_info(self, vm_name: str) -> VMInfo:
        """Get detailed info for a specific VM by name.

        Searches across all datacenters.
        """
        container = self.client.get_container_view([vim.VirtualMachine])

        for vm_obj in container.view:
            if vm_obj.name == vm_name:
                container.Destroy()
                return self._extract_vm_info(vm_obj)

        container.Destroy()
        raise ValueError(f"VM '{vm_name}' not found in vCenter")

    def get_vm_by_pattern(self, pattern: str) -> list[VMInfo]:
        """Get VMs matching a name pattern (supports * wildcard)."""
        import fnmatch
        all_vms = self.list_all_vms()
        return [vm for vm in all_vms if fnmatch.fnmatch(vm.name, pattern)]

    def _extract_vm_info(self, vm: vim.VirtualMachine) -> VMInfo:
        """Extract all relevant information from a VM managed object."""
        config = vm.config
        summary = vm.summary
        runtime = vm.runtime

        # Basic info
        info = VMInfo(
            name=vm.name,
            uuid=config.uuid if config else "",
            cpu=config.hardware.numCPU if config else 0,
            memory_mb=config.hardware.memoryMB if config else 0,
            power_state=str(runtime.powerState) if runtime else "unknown",
            guest_os=config.guestId if config else "unknown",
            guest_os_full=config.guestFullName if config else None,
            firmware=config.firmware if config and hasattr(config, 'firmware') else "bios",
            annotation=config.annotation if config and config.annotation else "",
        )

        # Host info
        if runtime and runtime.host:
            info.host = runtime.host.name
            # Navigate up to find datacenter and cluster
            parent = runtime.host.parent
            while parent:
                if isinstance(parent, vim.ClusterComputeResource):
                    info.cluster = parent.name
                elif isinstance(parent, vim.Datacenter):
                    info.datacenter = parent.name
                    break
                parent = getattr(parent, 'parent', None)

        # VMware Tools status
        if vm.guest:
            info.tools_status = vm.guest.toolsStatus or "unknown"
            info.tools_version = vm.guest.toolsVersion or ""

        # Disks and NICs
        if config and config.hardware:
            info.disks = self._extract_disks(config.hardware.device)
            info.nics = self._extract_nics(config.hardware.device, vm.guest)

        # Snapshots
        if vm.snapshot and vm.snapshot.rootSnapshotList:
            info.snapshots = self._extract_snapshot_names(vm.snapshot.rootSnapshotList)

        return info

    def _extract_disks(self, devices: list) -> list[DiskInfo]:
        """Extract disk information from VM hardware devices."""
        disks = []
        controllers = {}

        # First pass: map controller keys to types
        for device in devices:
            if isinstance(device, vim.vm.device.VirtualSCSIController):
                controllers[device.key] = "scsi"
            elif isinstance(device, vim.vm.device.VirtualNVMEController):
                controllers[device.key] = "nvme"
            elif isinstance(device, vim.vm.device.VirtualIDEController):
                controllers[device.key] = "ide"

        # Second pass: extract disk info
        for device in devices:
            if isinstance(device, vim.vm.device.VirtualDisk):
                backing = device.backing
                datastore = ""
                path = ""
                thin = False

                if hasattr(backing, 'fileName'):
                    path = backing.fileName
                if hasattr(backing, 'datastore') and backing.datastore:
                    datastore = backing.datastore.name
                if hasattr(backing, 'thinProvisioned'):
                    thin = backing.thinProvisioned

                controller_type = controllers.get(device.controllerKey, "unknown")

                disks.append(DiskInfo(
                    name=device.deviceInfo.label if device.deviceInfo else f"disk-{device.key}",
                    size_gb=round(device.capacityInKB / 1024 / 1024, 2),
                    thin_provisioned=thin,
                    datastore=datastore,
                    path=path,
                    controller_type=controller_type,
                    key=device.key,
                ))

        return disks

    def _extract_nics(self, devices: list, guest: vim.vm.GuestInfo | None) -> list[NICInfo]:
        """Extract NIC information from VM hardware devices."""
        nics = []

        # Build IP map from guest info
        ip_map: dict[str, list[str]] = {}
        if guest and guest.net:
            for net_info in guest.net:
                if net_info.macAddress and net_info.ipAddress:
                    ip_map[net_info.macAddress] = list(net_info.ipAddress)

        for device in devices:
            if isinstance(device, vim.vm.device.VirtualEthernetCard):
                adapter_type = type(device).__name__.replace("Virtual", "").lower()
                # Normalize common types
                type_map = {
                    "vmxnet3": "vmxnet3",
                    "e1000": "e1000",
                    "e1000e": "e1000e",
                    "vmxnet2": "vmxnet2",
                    "vmxnet": "vmxnet",
                }
                adapter_type = type_map.get(adapter_type, adapter_type)

                network = ""
                if hasattr(device.backing, 'network') and device.backing.network:
                    network = device.backing.network.name
                elif hasattr(device.backing, 'port'):
                    network = f"dvs-{device.backing.port.portgroupKey}"

                mac = device.macAddress or ""
                nics.append(NICInfo(
                    mac_address=mac,
                    network=network,
                    adapter_type=adapter_type,
                    connected=device.connectable.connected if device.connectable else False,
                    ip_addresses=ip_map.get(mac, []),
                ))

        return nics

    def _extract_snapshot_names(self, snapshot_list, prefix: str = "") -> list[str]:
        """Recursively extract snapshot names."""
        names = []
        for snap in snapshot_list:
            full_name = f"{prefix}/{snap.name}" if prefix else snap.name
            names.append(full_name)
            if snap.childSnapshotList:
                names.extend(self._extract_snapshot_names(snap.childSnapshotList, full_name))
        return names
