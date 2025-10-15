"""Integration layer for applying dataset configurations to BigQuery datasets."""

from typing import Any, Dict, Optional

from google.cloud.bigquery import Dataset, DatasetReference

from dbt.adapters.bigquery.dataset_config import DatasetConfigManager
from dbt.adapters.events.logging import AdapterLogger


logger = AdapterLogger("BigQuery")


class DatasetConfigApplier:
    """Applies dataset configurations to BigQuery Dataset objects."""

    def __init__(self, config_manager: Optional[DatasetConfigManager] = None):
        """Initialize with optional DatasetConfigManager."""
        self.config_manager = config_manager or DatasetConfigManager()

    def apply_config_to_dataset(
        self,
        dataset: Dataset,
        schema_name: str,
    ) -> Dataset:
        """Apply dataset configuration to a BigQuery Dataset object.

        Args:
            dataset: The BigQuery Dataset object to configure
            schema_name: The schema/dataset name to look up configuration for

        Returns:
            The configured Dataset object
        """
        config = self.config_manager.get_config(schema_name)

        if not config:
            logger.debug(f"No dataset configuration found for schema '{schema_name}'")
            return dataset

        logger.debug(f"Applying dataset configuration for '{schema_name}'")

        # Apply location
        if config.location:
            dataset.location = config.location

        # Apply description
        if config.description:
            dataset.description = config.description

        # Apply labels
        if config.labels:
            dataset.labels = config.labels

        # Apply table expiration
        if config.default_table_expiration_ms:
            dataset.default_table_expiration_ms = config.default_table_expiration_ms

        # Apply partition expiration
        if config.default_partition_expiration_ms:
            dataset.default_partition_expiration_ms = config.default_partition_expiration_ms

        return dataset

    def create_dataset_with_config(
        self,
        project: str,
        dataset_id: str,
        schema_name: str,
    ) -> Dataset:
        """Create a new Dataset object with configuration applied.

        Args:
            project: GCP project ID
            dataset_id: BigQuery dataset ID
            schema_name: The schema name to look up configuration for

        Returns:
            A new Dataset object with configuration applied
        """
        dataset_ref = DatasetReference(project, dataset_id)
        dataset = Dataset(dataset_ref)

        return self.apply_config_to_dataset(dataset, schema_name)

    def should_update_dataset(
        self,
        existing_dataset: Dataset,
        schema_name: str,
    ) -> bool:
        """Check if an existing dataset needs to be updated based on configuration.

        Args:
            existing_dataset: The existing BigQuery Dataset object
            schema_name: The schema name to look up configuration for

        Returns:
            True if the dataset should be updated, False otherwise
        """
        config = self.config_manager.get_config(schema_name)

        if not config:
            return False

        # Check if any configuration values differ
        if config.location and existing_dataset.location != config.location:
            return True

        if config.description and existing_dataset.description != config.description:
            return True

        if config.labels:
            existing_labels = existing_dataset.labels or {}
            if existing_labels != config.labels:
                return True

        if config.default_table_expiration_ms:
            if existing_dataset.default_table_expiration_ms != config.default_table_expiration_ms:
                return True

        if config.default_partition_expiration_ms:
            existing_exp = existing_dataset.default_partition_expiration_ms
            if existing_exp != config.default_partition_expiration_ms:
                return True

        return False

    def get_replication_config(self, schema_name: str) -> Optional[Dict[str, Any]]:
        """Get replication configuration for a schema if configured.

        Args:
            schema_name: The schema name to look up configuration for

        Returns:
            Replication configuration dictionary or None
        """
        config = self.config_manager.get_config(schema_name)

        if not config or not config.has_replication():
            return None

        return config.replication.to_dict()
