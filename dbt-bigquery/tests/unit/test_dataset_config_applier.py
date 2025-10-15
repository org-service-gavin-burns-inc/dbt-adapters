"""Unit tests for dataset configuration applier."""

import pytest
from unittest.mock import Mock
from google.cloud.bigquery import Dataset, DatasetReference

from dbt.adapters.bigquery.dataset_config import (
    BigQueryReplicationConfig,
    BigQueryDatasetConfig,
    DatasetConfigManager,
)
from dbt.adapters.bigquery.dataset_config_applier import DatasetConfigApplier


class TestDatasetConfigApplier:
    """Tests for DatasetConfigApplier."""
    
    def test_apply_config_no_config(self):
        """Test applying config when no configuration exists."""
        applier = DatasetConfigApplier()
        dataset_ref = DatasetReference('project', 'dataset')
        dataset = Dataset(dataset_ref)
        dataset.location = 'US'
        
        result = applier.apply_config_to_dataset(dataset, 'unknown_schema')
        
        # Should return unchanged dataset
        assert result.location == 'US'
    
    def test_apply_config_with_location(self):
        """Test applying configuration with location."""
        datasets_config = {
            'analytics': {
                'location': 'EU',
                'labels': {'env': 'prod'}
            }
        }
        manager = DatasetConfigManager(datasets_config)
        applier = DatasetConfigApplier(manager)
        
        dataset_ref = DatasetReference('project', 'dataset')
        dataset = Dataset(dataset_ref)
        
        result = applier.apply_config_to_dataset(dataset, 'analytics')
        
        assert result.location == 'EU'
        assert result.labels == {'env': 'prod'}
    
    def test_apply_config_with_all_options(self):
        """Test applying configuration with all options."""
        datasets_config = {
            'analytics': {
                'location': 'US',
                'description': 'Analytics dataset',
                'labels': {'env': 'prod', 'tier': 'critical'},
                'default_table_expiration_ms': 86400000,
                'default_partition_expiration_ms': 2592000000
            }
        }
        manager = DatasetConfigManager(datasets_config)
        applier = DatasetConfigApplier(manager)
        
        dataset_ref = DatasetReference('project', 'dataset')
        dataset = Dataset(dataset_ref)
        
        result = applier.apply_config_to_dataset(dataset, 'analytics')
        
        assert result.location == 'US'
        assert result.description == 'Analytics dataset'
        assert result.labels == {'env': 'prod', 'tier': 'critical'}
        assert result.default_table_expiration_ms == 86400000
        assert result.default_partition_expiration_ms == 2592000000
    
    def test_create_dataset_with_config(self):
        """Test creating a new dataset with configuration."""
        datasets_config = {
            'staging': {
                'location': 'EU',
                'labels': {'env': 'dev'}
            }
        }
        manager = DatasetConfigManager(datasets_config)
        applier = DatasetConfigApplier(manager)
        
        result = applier.create_dataset_with_config('my-project', 'my_dataset', 'staging')
        
        assert result.project == 'my-project'
        assert result.dataset_id == 'my_dataset'
        assert result.location == 'EU'
        assert result.labels == {'env': 'dev'}
    
    def test_should_update_dataset_no_changes(self):
        """Test checking for updates when no changes needed."""
        datasets_config = {
            'analytics': {
                'location': 'US',
                'labels': {'env': 'prod'}
            }
        }
        manager = DatasetConfigManager(datasets_config)
        applier = DatasetConfigApplier(manager)
        
        dataset_ref = DatasetReference('project', 'dataset')
        dataset = Dataset(dataset_ref)
        dataset.location = 'US'
        dataset.labels = {'env': 'prod'}
        
        result = applier.should_update_dataset(dataset, 'analytics')
        
        assert result is False
    
    def test_should_update_dataset_location_changed(self):
        """Test checking for updates when location changed."""
        datasets_config = {
            'analytics': {
                'location': 'EU',
                'labels': {'env': 'prod'}
            }
        }
        manager = DatasetConfigManager(datasets_config)
        applier = DatasetConfigApplier(manager)
        
        dataset_ref = DatasetReference('project', 'dataset')
        dataset = Dataset(dataset_ref)
        dataset.location = 'US'
        dataset.labels = {'env': 'prod'}
        
        result = applier.should_update_dataset(dataset, 'analytics')
        
        assert result is True
    
    def test_should_update_dataset_labels_changed(self):
        """Test checking for updates when labels changed."""
        datasets_config = {
            'analytics': {
                'location': 'US',
                'labels': {'env': 'prod', 'tier': 'critical'}
            }
        }
        manager = DatasetConfigManager(datasets_config)
        applier = DatasetConfigApplier(manager)
        
        dataset_ref = DatasetReference('project', 'dataset')
        dataset = Dataset(dataset_ref)
        dataset.location = 'US'
        dataset.labels = {'env': 'prod'}
        
        result = applier.should_update_dataset(dataset, 'analytics')
        
        assert result is True
    
    def test_should_update_dataset_no_config(self):
        """Test checking for updates when no configuration exists."""
        applier = DatasetConfigApplier()
        
        dataset_ref = DatasetReference('project', 'dataset')
        dataset = Dataset(dataset_ref)
        dataset.location = 'US'
        
        result = applier.should_update_dataset(dataset, 'unknown_schema')
        
        assert result is False
    
    def test_get_replication_config_exists(self):
        """Test getting replication configuration when it exists."""
        datasets_config = {
            'analytics': {
                'location': 'US',
                'replication': {
                    'enabled': True,
                    'replicas': ['us-east1', 'us-west1'],
                    'primary_location': 'us-central1'
                }
            }
        }
        manager = DatasetConfigManager(datasets_config)
        applier = DatasetConfigApplier(manager)
        
        result = applier.get_replication_config('analytics')
        
        assert result is not None
        assert result['enabled'] is True
        assert result['replicas'] == ['us-east1', 'us-west1']
        assert result['primary_location'] == 'us-central1'
    
    def test_get_replication_config_disabled(self):
        """Test getting replication configuration when disabled."""
        datasets_config = {
            'staging': {
                'location': 'US',
                'replication': {
                    'enabled': False
                }
            }
        }
        manager = DatasetConfigManager(datasets_config)
        applier = DatasetConfigApplier(manager)
        
        result = applier.get_replication_config('staging')
        
        assert result is None
    
    def test_get_replication_config_not_configured(self):
        """Test getting replication configuration when not configured."""
        datasets_config = {
            'staging': {
                'location': 'US'
            }
        }
        manager = DatasetConfigManager(datasets_config)
        applier = DatasetConfigApplier(manager)
        
        result = applier.get_replication_config('staging')
        
        assert result is None
