"""Configuration classes for BigQuery dataset management."""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
import hashlib
import json


@dataclass
class BigQueryReplicationConfig:
    """Replication configuration for a BigQuery dataset."""

    enabled: bool = True
    replicas: List[str] = field(default_factory=list)
    primary_location: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "enabled": self.enabled,
            "replicas": self.replicas,
            "primary_location": self.primary_location,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BigQueryReplicationConfig":
        """Create from dictionary representation."""
        return cls(
            enabled=data.get("enabled", True),
            replicas=data.get("replicas", []),
            primary_location=data.get("primary_location"),
        )


@dataclass
class BigQueryDatasetConfig:
    """Complete configuration for a BigQuery dataset."""

    name: str
    location: str = "US"
    replication: Optional[BigQueryReplicationConfig] = None
    labels: Dict[str, str] = field(default_factory=dict)
    description: Optional[str] = None
    default_table_expiration_ms: Optional[int] = None
    default_partition_expiration_ms: Optional[int] = None

    def __post_init__(self):
        """Validate and normalize configuration after initialization."""
        if self.replication and isinstance(self.replication, dict):
            self.replication = BigQueryReplicationConfig.from_dict(self.replication)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result: Dict[str, Any] = {
            "name": self.name,
            "location": self.location,
            "labels": self.labels,
        }

        if self.replication:
            result["replication"] = self.replication.to_dict()

        if self.description:
            result["description"] = self.description

        if self.default_table_expiration_ms:
            result["default_table_expiration_ms"] = self.default_table_expiration_ms

        if self.default_partition_expiration_ms:
            result["default_partition_expiration_ms"] = self.default_partition_expiration_ms

        return result

    @classmethod
    def from_dict(cls, name: str, data: Dict[str, Any]) -> "BigQueryDatasetConfig":
        """Create from dictionary representation."""
        replication_data = data.get("replication")
        replication = None
        if replication_data:
            replication = BigQueryReplicationConfig.from_dict(replication_data)

        return cls(
            name=name,
            location=data.get("location", "US"),
            replication=replication,
            labels=data.get("labels", {}),
            description=data.get("description"),
            default_table_expiration_ms=data.get("default_table_expiration_ms"),
            default_partition_expiration_ms=data.get("default_partition_expiration_ms"),
        )

    def compute_hash(self) -> str:
        """Compute a hash of the configuration for change detection."""
        config_str = json.dumps(self.to_dict(), sort_keys=True)
        return hashlib.sha256(config_str.encode()).hexdigest()[:16]

    def has_replication(self) -> bool:
        """Check if replication is configured and enabled."""
        return self.replication is not None and self.replication.enabled

    def validate(self) -> List[str]:
        """Validate the configuration and return list of errors."""
        errors = []

        if not self.name:
            errors.append("Dataset name is required")

        if not self.location:
            errors.append("Dataset location is required")

        if self.has_replication():
            assert self.replication is not None  # Type narrowing for mypy
            if not self.replication.replicas:
                errors.append(
                    f"Dataset '{self.name}': replication enabled but " "no replicas specified"
                )

            if not self.replication.primary_location:
                errors.append(
                    f"Dataset '{self.name}': replication enabled but "
                    "no primary_location specified"
                )

            # Validate replica locations are valid
            for replica in self.replication.replicas:
                if not replica or not isinstance(replica, str):
                    errors.append(
                        f"Dataset '{self.name}': invalid replica " f"location: {replica}"
                    )

        # Validate labels
        for key, value in self.labels.items():
            if not isinstance(key, str) or not isinstance(value, str):
                errors.append(f"Dataset '{self.name}': labels must be string key-value pairs")

        return errors


class DatasetConfigManager:
    """Manager for dataset configurations from dbt_project.yml."""

    def __init__(self, datasets_config: Optional[Dict[str, Any]] = None):
        """Initialize with datasets configuration from dbt_project.yml."""
        self.configs: Dict[str, BigQueryDatasetConfig] = {}

        if datasets_config:
            self._parse_configs(datasets_config)

    def _parse_configs(self, datasets_config: Dict[str, Any]) -> None:
        """Parse dataset configurations from raw config dictionary."""
        for name, config_data in datasets_config.items():
            if not isinstance(config_data, dict):
                continue

            config = BigQueryDatasetConfig.from_dict(name, config_data)
            self.configs[name] = config

    def get_config(self, schema_name: str) -> Optional[BigQueryDatasetConfig]:
        """Get dataset configuration for a schema name."""
        return self.configs.get(schema_name)

    def has_config(self, schema_name: str) -> bool:
        """Check if configuration exists for a schema name."""
        return schema_name in self.configs

    def validate_all(self) -> List[str]:
        """Validate all configurations and return list of errors."""
        all_errors = []
        for config in self.configs.values():
            errors = config.validate()
            all_errors.extend(errors)
        return all_errors

    def get_all_configs(self) -> Dict[str, BigQueryDatasetConfig]:
        """Get all dataset configurations."""
        return self.configs.copy()
