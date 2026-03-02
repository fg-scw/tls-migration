"""VMware snapshot management for migration."""

from __future__ import annotations

from pyVmomi import vim

from vmware2scw.utils.logging import get_logger
from vmware2scw.vmware.client import VSphereClient

logger = get_logger(__name__)

SNAPSHOT_PREFIX = "vmware2scw-"


class SnapshotManager:
    """Manage VMware snapshots for migration purposes.

    Creates a consistent snapshot before disk export and cleans up
    after migration completes or fails.

    Confidence: 92 — vSphere snapshot API is well-documented and stable.
    """

    def __init__(self, client: VSphereClient):
        self.client = client

    def create_migration_snapshot(
        self,
        vm_name: str,
        snapshot_name: str,
        description: str = "Snapshot for vmware2scw migration",
        memory: bool = False,
        quiesce: bool = True,
    ) -> str:
        """Create a snapshot of a VM for consistent disk export."""
        vm_obj = self._get_vm(vm_name)

        logger.info(f"Creating snapshot '{snapshot_name}' for VM '{vm_name}'...")
        try:
            task = vm_obj.CreateSnapshot_Task(
                name=snapshot_name,
                description=description,
                memory=memory,
                quiesce=quiesce,
            )
            self.client.wait_for_task(task, timeout=600)
            logger.info(f"Snapshot '{snapshot_name}' created successfully")
            return snapshot_name
        except Exception as e:
            if quiesce and "quiesce" in str(e).lower():
                logger.warning("Quiesced snapshot failed — retrying without quiesce")
                task = vm_obj.CreateSnapshot_Task(
                    name=snapshot_name, description=description,
                    memory=memory, quiesce=False,
                )
                self.client.wait_for_task(task, timeout=600)
                return snapshot_name
            raise RuntimeError(f"Failed to create snapshot: {e}")

    def delete_migration_snapshot(self, vm_name: str, snapshot_name: str) -> None:
        """Delete a migration snapshot by name."""
        vm_obj = self._get_vm(vm_name)
        if not vm_obj.snapshot or not vm_obj.snapshot.rootSnapshotList:
            return
        snap_ref = self._find_snapshot(vm_obj.snapshot.rootSnapshotList, snapshot_name)
        if snap_ref is None:
            logger.warning(f"Snapshot '{snapshot_name}' not found")
            return
        task = snap_ref.RemoveSnapshot_Task(removeChildren=False)
        self.client.wait_for_task(task, timeout=600)
        logger.info(f"Snapshot '{snapshot_name}' deleted")

    def _get_vm(self, vm_name: str):
        container = self.client.get_container_view([vim.VirtualMachine])
        for vm in container.view:
            if vm.name == vm_name:
                container.Destroy()
                return vm
        container.Destroy()
        raise ValueError(f"VM '{vm_name}' not found")

    def _find_snapshot(self, snapshot_list, name: str):
        for snap in snapshot_list:
            if snap.name == name:
                return snap.snapshot
            if snap.childSnapshotList:
                result = self._find_snapshot(snap.childSnapshotList, name)
                if result:
                    return result
        return None