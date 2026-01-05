"""
ADLS Mount Manager -
Manages mounts in Databricks providing a scalable solution for mounting multiple ADLS containers
with proper error handling, logging, and credential management.

Usage:
    from src.utils.mount_manager import ADLSMountManager

    mount_manager = ADLSMountManager(
        dbutils=dbutils,
        key_vault_scope='my-scope',
        client_id_key='app-client-id',
        tenant_id_key='app-tenant-id',
        client_secret_key='app-secret-id'
    )
    mount_manager.mount_containers('mystorageaccount', ['raw', 'processed'])
"""

from typing import List, Dict, Optional
import logging


class ADLSMountManager:
    """
    Manages ADLS container mounts in Databricks.
    Provides a scalable solution for mounting multiple containers with proper error handling.
    """

    def __init__(
        self,
        dbutils,
        key_vault_scope: str,
        client_id_key: str,
        tenant_id_key: str,
        client_secret_key: str,
    ):
        """
        Initialize the mount manager with Key Vault credentials.

        Args:
            dbutils: Databricks utilities object (passed from notebook).
            key_vault_scope: The Key Vault scope name for retrieving secrets.
            client_id_key: The secret key name for the Azure AD application client ID.
            tenant_id_key: The secret key name for the Azure AD tenant ID.
            client_secret_key: The secret key name for the Azure AD application secret.
        """
        self._dbutils = dbutils
        self.key_vault_scope = key_vault_scope
        self._client_id_key = client_id_key
        self._tenant_id_key = tenant_id_key
        self._client_secret_key = client_secret_key
        self.logger = self._setup_logger()
        self._credentials = self._load_credentials()

    def _setup_logger(self) -> logging.Logger:
        """Configure logging for mount operations."""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        return logger

    def _load_credentials(self) -> Dict[str, str]:
        """Load credentials from Key Vault once to avoid multiple calls."""
        try:
            return {
                "client_id": self._dbutils.secrets.get(
                    scope=self.key_vault_scope, key=self._client_id_key
                ),
                "tenant_id": self._dbutils.secrets.get(
                    scope=self.key_vault_scope, key=self._tenant_id_key
                ),
                "client_secret": self._dbutils.secrets.get(
                    scope=self.key_vault_scope, key=self._client_secret_key
                ),
            }
        except Exception as e:
            self.logger.error(f"Failed to load credentials from Key Vault: {str(e)}")
            raise

    def _get_mount_configs(self) -> Dict[str, str]:
        """Generate OAuth configuration for ADLS mount."""
        return {
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": self._credentials["client_id"],
            "fs.azure.account.oauth2.client.secret": self._credentials["client_secret"],
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{self._credentials['tenant_id']}/oauth2/token",
        }

    def _is_mounted(self, mount_point: str) -> bool:
        """Check if a mount point already exists."""
        return any(
            mount.mountPoint == mount_point for mount in self._dbutils.fs.mounts()
        )

    def _mount_container(
        self,
        storage_account_name: str,
        container_name: str,
        force_remount: bool = False,
    ) -> bool:
        """
        Mount a single ADLS container.

        Args:
            storage_account_name: Azure storage account name
            container_name: Container name to mount
            force_remount: If True, unmount and remount if already mounted

        Returns:
            bool: True if mount successful, False otherwise
        """
        mount_point = f"/mnt/{storage_account_name}/{container_name}"
        source = (
            f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"
        )

        try:
            # Check if already mounted
            if self._is_mounted(mount_point):
                if force_remount:
                    self.logger.info(f"Unmounting existing mount: {mount_point}")
                    self._dbutils.fs.unmount(mount_point)
                else:
                    self.logger.info(
                        f"Mount point already exists: {mount_point}. Skipping."
                    )
                    return True

            # Perform mount
            self.logger.info(f"Mounting {source} to {mount_point}")
            self._dbutils.fs.mount(
                source=source,
                mount_point=mount_point,
                extra_configs=self._get_mount_configs(),
            )
            self.logger.info(f"Successfully mounted: {mount_point}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to mount {container_name}: {str(e)}")
            return False

    def mount_containers(
        self,
        storage_account_name: str,
        container_names: List[str],
        force_remount: bool = False,
    ) -> Dict[str, bool]:
        """
        Mount one or more containers from the same storage account.

        Args:
            storage_account_name: Azure storage account name
            container_names: List of container names to mount
            force_remount: If True, unmount and remount if already mounted

        Returns:
            Dict[str, bool]: Dictionary mapping container names to mount success status
        """
        results = {}
        for container in container_names:
            results[container] = self._mount_container(
                storage_account_name, container, force_remount
            )

        # Summary
        successful = sum(1 for status in results.values() if status)
        self.logger.info(
            f"Mount summary: {successful}/{len(container_names)} successful"
        )

        return results

    def unmount_container(self, storage_account_name: str, container_name: str) -> bool:
        """
        Unmount a specific container.

        Args:
            storage_account_name: Azure storage account name
            container_name: Container name to unmount

        Returns:
            bool: True if unmount successful, False otherwise
        """
        mount_point = f"/mnt/{storage_account_name}/{container_name}"

        try:
            if self._is_mounted(mount_point):
                self._dbutils.fs.unmount(mount_point)
                self.logger.info(f"Successfully unmounted: {mount_point}")
                return True
            else:
                self.logger.info(f"Mount point does not exist: {mount_point}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to unmount {mount_point}: {str(e)}")
            return False

    def unmount_containers(
        self, storage_account_name: str, container_names: List[str]
    ) -> Dict[str, bool]:
        """
        Unmount one or more containers from a storage account.

        Args:
            storage_account_name: Azure storage account name
            container_names: List of container names to unmount

        Returns:
            Dict[str, bool]: Dictionary mapping container names to unmount success status
        """
        results = {}
        for container in container_names:
            results[container] = self.unmount_container(storage_account_name, container)

        successful = sum(1 for status in results.values() if status)
        self.logger.info(
            f"Unmount summary: {successful}/{len(container_names)} successful"
        )

        return results

    def list_mounts(self, storage_account_name: Optional[str] = None) -> list:
        """
        Get all mounts or filtered by storage account.

        Args:
            storage_account_name: Optional filter for specific storage account

        Returns:
            list: List of mount info objects
        """
        mounts = self._dbutils.fs.mounts()

        if storage_account_name:
            return [
                m for m in mounts if f"/mnt/{storage_account_name}/" in m.mountPoint
            ]
        return list(mounts)
