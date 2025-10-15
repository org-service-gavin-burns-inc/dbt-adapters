"""Unit tests for dataset configuration classes."""

import pytest
from dbt.adapters.bigquery.dataset_config import (
    BigQueryReplicationConfig,
    BigQueryDatasetConfig,
    DatasetConfigManager,
)


class TestBigQueryReplicationConfig:
    """Tests for BigQueryReplicationConfig."""

    def test_default_config(self):
        """Test default replication configuration."""
        config = BigQueryReplicationConfig()
        assert config.enabled is True
        assert config.replicas == []
        assert config.primary_location is None

    def test_full_config(self):
        """Test full replication configuration."""
        config = BigQueryReplicationConfig(
            enabled=True,
            replicas=['us-east1', 'us-west1'],
            primary_location='us-central1'
        )
        assert config.enabled is True
        assert config.replicas == ['us-east1', 'us-west1']
        assert config.primary_location == 'us-central1'

    def test_to_dict(self):
        """Test conversion to dictionary."""
        config = BigQueryReplicationConfig(
            enabled=True,
            replicas=['us-east1'],
            primary_location='us-central1'
        )
        result = config.to_dict()
        assert result['enabled'] is True
        assert result['replicas'] == ['us-east1']
        assert result['primary_location'] == 'us-central1'

    def test_from_dict(self):
        """Test creation from dictionary."""
        data = {
            'enabled': True,
            'replicas': ['us-east1', 'us-west1'],
            'primary_location': 'us-central1'
        }
        config = BigQueryReplicationConfig.from_dict(data)
        assert config.enabled is True
        assert config.replicas == ['us-east1', 'us-west1']
        assert config.primary_location == 'us-central1'


class TestBigQueryDatasetConfig:
    """Tests for BigQueryDatasetConfig."""

    def test_minimal_config(self):
        """Test minimal dataset configuration."""
        config = BigQueryDatasetConfig(name='test_dataset')
        assert config.name == 'test_dataset'
        assert config.location == 'US'
        assert config.replication is None
        assert config.labels == {}

    def test_full_config(self):
        """Test full dataset configuration."""
        replication = BigQueryReplicationConfig(
            enabled=True,
            replicas=['us-east1'],
            primary_location='us-central1'
        )
        config = BigQueryDatasetConfig(
            name='analytics',
            location='US',
            replication=replication,
            labels={'env': 'prod', 'tier': 'critical'},
            description='Analytics dataset',
            default_table_expiration_ms=86400000
        )
        assert config.name == 'analytics'
        assert config.location == 'US'
        assert config.has_replication() is True
        assert config.labels == {'env': 'prod', 'tier': 'critical'}
        assert config.description == 'Analytics dataset'

    def test_from_dict_with_replication(self):
        """Test creation from dictionary with replication."""
        data = {
            'location': 'US',
            'replication': {
                'enabled': True,
                'replicas': ['us-east1', 'us-west1'],
                'primary_location': 'us-central1'
            },
            'labels': {'env': 'prod'}
        }
        config = BigQueryDatasetConfig.from_dict('test_dataset', data)
        assert config.name == 'test_dataset'
        assert config.location == 'US'
        assert config.has_replication() is True
        assert config.replication.replicas == ['us-east1', 'us-west1']
        assert config.labels == {'env': 'prod'}

    def test_from_dict_without_replication(self):
        """Test creation from dictionary without replication."""
        data = {
            'location': 'EU',
            'labels': {'env': 'dev'}
        }
        config = BigQueryDatasetConfig.from_dict('staging', data)
        assert config.name == 'staging'
        assert config.location == 'EU'
        assert config.has_replication() is False

    def test_compute_hash(self):
        """Test hash computation for change detection."""
        config1 = BigQueryDatasetConfig(name='test', location='US')
        hash1 = config1.compute_hash()
        assert len(hash1) == 16

        # Same config should produce same hash
        config2 = BigQueryDatasetConfig(name='test', location='US')
        hash2 = config2.compute_hash()
        assert hash1 == hash2

        # Different config should produce different hash
        config3 = BigQueryDatasetConfig(name='test', location='EU')
        hash3 = config3.compute_hash()
        assert hash1 != hash3

    def test_validate_minimal_valid(self):
        """Test validation of minimal valid configuration."""
        config = BigQueryDatasetConfig(name='test', location='US')
        errors = config.validate()
        assert len(errors) == 0

    def test_validate_missing_name(self):
        """Test validation catches missing name."""
        config = BigQueryDatasetConfig(name='', location='US')
        errors = config.validate()
        assert len(errors) > 0
        assert any('name is required' in err for err in errors)

    def test_validate_replication_no_replicas(self):
        """Test validation catches replication without replicas."""
        replication = BigQueryReplicationConfig(
            enabled=True,
            replicas=[],
            primary_location='us-central1'
        )
        config = BigQueryDatasetConfig(
            name='test',
            location='US',
            replication=replication
        )
        errors = config.validate()
        assert len(errors) > 0
        assert any('no replicas specified' in err for err in errors)

    def test_validate_replication_no_primary(self):
        """Test validation catches replication without primary location."""
        replication = BigQueryReplicationConfig(
            enabled=True,
            replicas=['us-east1'],
            primary_location=None
        )
        config = BigQueryDatasetConfig(
            name='test',
            location='US',
            replication=replication
        )
        errors = config.validate()
        assert len(errors) > 0
        assert any('no primary_location specified' in err for err in errors)


class TestDatasetConfigManager:
    """Tests for DatasetConfigManager."""

    def test_empty_manager(self):
        """Test empty manager initialization."""
        manager = DatasetConfigManager()
        assert len(manager.configs) == 0
        assert manager.get_config('test') is None
        assert not manager.has_config('test')

    def test_parse_simple_configs(self):
        """Test parsing simple dataset configurations."""
        datasets_config = {
            'analytics': {
                'location': 'US',
                'labels': {'env': 'prod'}
            },
            'staging': {
                'location': 'EU'
            }
        }
        manager = DatasetConfigManager(datasets_config)

        assert len(manager.configs) == 2
        assert manager.has_config('analytics')
        assert manager.has_config('staging')

        analytics_config = manager.get_config('analytics')
        assert analytics_config.name == 'analytics'
        assert analytics_config.location == 'US'
        assert analytics_config.labels == {'env': 'prod'}

    def test_parse_config_with_replication(self):
        """Test parsing configuration with replication."""
        datasets_config = {
            'analytics': {
                'location': 'US',
                'replication': {
                    'enabled': True,
                    'replicas': ['us-east1', 'us-west1'],
                    'primary_location': 'us-central1'
                },
                'labels': {'env': 'prod', 'tier': 'critical'}
            }
        }
        manager = DatasetConfigManager(datasets_config)

        config = manager.get_config('analytics')
        assert config is not None
        assert config.has_replication() is True
        assert config.replication.replicas == ['us-east1', 'us-west1']
        assert config.replication.primary_location == 'us-central1'

    def test_get_all_configs(self):
        """Test getting all configurations."""
        datasets_config = {
            'analytics': {'location': 'US'},
            'staging': {'location': 'EU'}
        }
        manager = DatasetConfigManager(datasets_config)

        all_configs = manager.get_all_configs()
        assert len(all_configs) == 2
        assert 'analytics' in all_configs
        assert 'staging' in all_configs

    def test_validate_all_valid(self):
        """Test validation of all valid configurations."""
        datasets_config = {
            'analytics': {
                'location': 'US',
                'labels': {'env': 'prod'}
            },
            'staging': {
                'location': 'EU'
            }
        }
        manager = DatasetConfigManager(datasets_config)
        errors = manager.validate_all()
        assert len(errors) == 0

    def test_validate_all_with_errors(self):
        """Test validation catches errors across all configurations."""
        datasets_config = {
            'analytics': {
                'location': 'US',
                'replication': {
                    'enabled': True,
                    'replicas': [],  # Missing replicas
                    'primary_location': 'us-central1'
                }
            },
            'staging': {
                'location': ''  # Empty location
            }
        }
        manager = DatasetConfigManager(datasets_config)
        errors = manager.validate_all()
        assert len(errors) > 0
