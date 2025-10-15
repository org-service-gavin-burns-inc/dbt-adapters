from typing import Dict, List, Optional, Any

from google.cloud.bigquery import AccessEntry, Client, Dataset
from google.api_core import exceptions as google_exceptions

from dbt.adapters.events.logging import AdapterLogger


logger = AdapterLogger("BigQuery")


def is_access_entry_in_dataset(dataset: Dataset, access_entry: AccessEntry) -> bool:
    """Check if the access entry already exists in the dataset.

    Args:
        dataset (Dataset): the dataset to be updated
        access_entry (AccessEntry): the access entry to be added to the dataset

    Returns:
        bool: True if entry exists in dataset, False otherwise
    """
    access_entries: List[AccessEntry] = dataset.access_entries
    # we can't simply check if an access entry is in the list as the current equality check
    # does not work because the locally created AccessEntry can have extra properties.
    for existing_entry in access_entries:
        role_match = existing_entry.role == access_entry.role
        entity_type_match = existing_entry.entity_type == access_entry.entity_type
        property_match = existing_entry._properties.items() <= access_entry._properties.items()
        if role_match and entity_type_match and property_match:
            return True
    return False


def add_access_entry_to_dataset(dataset: Dataset, access_entry: AccessEntry) -> Dataset:
    """Adds an access entry to a dataset, always use access_entry_present_in_dataset to check
    if the access entry already exists before calling this function.

    Args:
        dataset (Dataset): the dataset to be updated
        access_entry (AccessEntry): the access entry to be added to the dataset

    Returns:
        Dataset: the updated dataset
    """
    access_entries: List[AccessEntry] = dataset.access_entries
    access_entries.append(access_entry)
    dataset.access_entries = access_entries
    return dataset


def get_dataset_replication_config(client: Client, project: str, dataset: str) -> Dict[str, Any]:
    """Query current replication configuration from INFORMATION_SCHEMA.

    Args:
        client (Client): BigQuery client
        project (str): GCP project ID
        dataset (str): Dataset name

    Returns:
        Dict[str, Any]: Dictionary with 'replicas' (list) and 'primary' (str or None)
    """
    query = f"""
    SELECT
        replica_location,
        is_primary_replica
    FROM `{project}.{dataset}`.INFORMATION_SCHEMA.SCHEMATA_REPLICAS
    WHERE schema_name = '{dataset}'
    """
    try:
        result = list(client.query(query).result())
        replicas = []
        primary = None
        for row in result:
            replicas.append(row.replica_location)
            if row.is_primary_replica:
                primary = row.replica_location
        return {"replicas": replicas, "primary": primary}
    except (google_exceptions.NotFound, google_exceptions.BadRequest) as exc:
        # Dataset doesn't exist or no replication configured
        logger.warning(f"Exception while fetching replication info for {project}.{dataset}: {exc}")
    return {"replicas": [], "primary": None}
        # Dataset doesn't exist or no replication configured
        pass
    return {"replicas": [], "primary": None}


def needs_replication_update(
    current_config: Dict[str, Any],
    desired_replicas: List[str],
    desired_primary: Optional[str] = None,
) -> bool:
    """Determine if replication configuration needs to be updated.

    Args:
        current_config (Dict[str, Any]): Current config from get_dataset_replication_config
        desired_replicas (List[str]): Desired replica locations
        desired_primary (Optional[str]): Desired primary replica location

    Returns:
        bool: True if update is needed, False otherwise
    """
    current_replicas = set(current_config.get("replicas", []))
    desired_replicas_set = set(desired_replicas)

    if current_replicas != desired_replicas_set:
        return True

    return bool(desired_primary and current_config.get("primary") != desired_primary)


def apply_dataset_replication(
    client: Client,
    project: str,
    dataset: str,
    desired_replicas: List[str],
    desired_primary: Optional[str] = None,
) -> None:
    """Apply replication configuration using ALTER SCHEMA DDL.

    Args:
        client (Client): BigQuery client
        project (str): GCP project ID
        dataset (str): Dataset name
        desired_replicas (List[str]): Desired replica locations
        desired_primary (Optional[str]): Desired primary replica location
    """
    # Get current state
    current = get_dataset_replication_config(client, project, dataset)

    # Check if update is needed
    if not needs_replication_update(current, desired_replicas, desired_primary):
        logger.debug(f"Dataset {project}.{dataset} replication already configured correctly")
        return

    logger.info(f"Configuring replication for dataset {project}.{dataset}")

    current_replicas = set(current["replicas"])
    desired_replicas_set = set(desired_replicas)

    # Add new replicas
    to_add = desired_replicas_set - current_replicas
    for location in to_add:
        sql = f"ALTER SCHEMA `{project}.{dataset}` ADD REPLICA `{location}`"
        logger.info(f"Adding replica: {location}")
        try:
            client.query(sql).result()
        except google_exceptions.GoogleAPIError as e:
            if "already exists" not in str(e).lower():
                logger.warning(f"Failed to add replica {location}: {e}")

    # Remove old replicas
    to_remove = current_replicas - desired_replicas_set
    for location in to_remove:
        sql = f"ALTER SCHEMA `{project}.{dataset}` DROP REPLICA `{location}`"
        logger.info(f"Dropping replica: {location}")
        try:
            client.query(sql).result()
        # Set primary replica if specified and different
        if desired_primary:
            if desired_primary not in desired_replicas_set:
                logger.warning(
                    f"Primary replica '{desired_primary}' is not in the desired replicas list; skipping primary update."
                )
            elif current["primary"] != desired_primary:
                sql = (
                    f"ALTER SCHEMA `{project}.{dataset}` "
                    f"SET OPTIONS (default_replica = `{desired_primary}`)"
                )
                logger.info(f"Setting primary replica: {desired_primary}")
                try:
                    client.query(sql).result()
                except google_exceptions.GoogleAPIError as e:
                    logger.warning(f"Failed to set primary replica: {e}")
            client.query(sql).result()
        except google_exceptions.GoogleAPIError as e:
            logger.warning(f"Failed to set primary replica: {e}")
