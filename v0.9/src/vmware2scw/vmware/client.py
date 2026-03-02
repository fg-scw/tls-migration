"""VMware vSphere/vCenter client connection and operations."""

from __future__ import annotations

import atexit
import hashlib
import ssl
import time
from typing import Optional

from pyVim.connect import Disconnect, SmartConnect
from pyVmomi import vim

from vmware2scw.utils.logging import get_logger

logger = get_logger(__name__)


class VSphereClient:
    """Manages connection to a VMware vSphere/vCenter instance.

    Uses pyvmomi to connect via the vSphere API. Supports:
    - SSL certificate verification bypass (common in enterprise)
    - Automatic retry with exponential backoff
    - Session management with cleanup on exit
    """

    def __init__(self):
        self._si: Optional[vim.ServiceInstance] = None
        self._content: Optional[vim.ServiceInstanceContent] = None
        self._host: str = ""

    @property
    def service_instance(self) -> vim.ServiceInstance:
        if self._si is None:
            raise ConnectionError("Not connected to vCenter. Call connect() first.")
        return self._si

    @property
    def content(self) -> vim.ServiceInstanceContent:
        if self._content is None:
            raise ConnectionError("Not connected to vCenter. Call connect() first.")
        return self._content

    def connect(
        self,
        host: str,
        username: str,
        password: str,
        port: int = 443,
        insecure: bool = False,
        max_retries: int = 3,
    ) -> vim.ServiceInstance:
        """Connect to vCenter/vSphere with retry logic.

        Args:
            host: vCenter hostname or IP address
            username: Login username (e.g. admin@vsphere.local)
            password: Login password
            port: API port (default 443)
            insecure: Skip SSL certificate verification
            max_retries: Number of connection attempts

        Returns:
            vSphere ServiceInstance

        Raises:
            ConnectionError: If all connection attempts fail
        """
        self._host = host
        ssl_context = None
        if insecure:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Connecting to vCenter {host} (attempt {attempt}/{max_retries})")
                self._si = SmartConnect(
                    host=host,
                    user=username,
                    pwd=password,
                    port=port,
                    sslContext=ssl_context,
                )
                self._content = self._si.RetrieveContent()
                atexit.register(Disconnect, self._si)

                logger.info(f"Connected to vCenter: {host} "
                            f"(API version: {self._content.about.apiVersion}, "
                            f"Build: {self._content.about.build})")
                return self._si

            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    delay = 2 ** attempt
                    logger.warning(f"Connection failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    logger.error(f"All {max_retries} connection attempts failed")

        raise ConnectionError(f"Failed to connect to vCenter {host}: {last_error}")

    def disconnect(self):
        """Gracefully disconnect from vCenter."""
        if self._si:
            try:
                Disconnect(self._si)
                logger.info(f"Disconnected from vCenter: {self._host}")
            except Exception as e:
                logger.warning(f"Error during disconnect: {e}")
            finally:
                self._si = None
                self._content = None

    def get_thumbprint(self) -> str:
        """Get SHA-1 thumbprint of the vCenter SSL certificate.

        Useful for VDDK/NBD connections that require certificate pinning.
        """
        import socket

        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        with socket.create_connection((self._host, 443)) as sock:
            with context.wrap_socket(sock, server_hostname=self._host) as ssock:
                cert_bin = ssock.getpeercert(True)
                if cert_bin is None:
                    raise ValueError("No certificate received from vCenter")
                sha1 = hashlib.sha1(cert_bin).hexdigest()
                thumbprint = ":".join(sha1[i:i+2] for i in range(0, len(sha1), 2))
                return thumbprint

    def get_container_view(self, obj_type: list, recursive: bool = True):
        """Create a container view for efficient object retrieval."""
        return self.content.viewManager.CreateContainerView(
            self.content.rootFolder, obj_type, recursive
        )

    def get_datacenters(self) -> list:
        """List all datacenters."""
        view = self.get_container_view([vim.Datacenter])
        dcs = list(view.view)
        view.Destroy()
        return dcs

    def wait_for_task(self, task: vim.Task, timeout: int = 600) -> None:
        """Wait for a vSphere task to complete.

        Args:
            task: vSphere Task object
            timeout: Maximum wait time in seconds

        Raises:
            RuntimeError: If task fails or times out
        """
        start = time.time()
        while task.info.state in (vim.TaskInfo.State.running, vim.TaskInfo.State.queued):
            if time.time() - start > timeout:
                raise TimeoutError(f"Task timed out after {timeout}s: {task.info.descriptionId}")
            time.sleep(2)

        if task.info.state == vim.TaskInfo.State.success:
            return
        elif task.info.state == vim.TaskInfo.State.error:
            raise RuntimeError(f"Task failed: {task.info.error.msg}")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.disconnect()
