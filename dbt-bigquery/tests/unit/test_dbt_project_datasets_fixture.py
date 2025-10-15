"""Test the dbt_project_datasets fixture."""

import pytest


def test_dbt_project_datasets_fixture_exists(dbt_project_datasets):
    """Test that the dbt_project_datasets fixture returns valid YAML."""
    assert dbt_project_datasets is not None
    assert isinstance(dbt_project_datasets, str)
    assert "name:" in dbt_project_datasets
    assert "test_datasets" in dbt_project_datasets
    assert "config-version:" in dbt_project_datasets


def test_dbt_project_datasets_fixture_contains_dataset_config(dbt_project_datasets):
    """Test that the fixture contains dataset-related configuration."""
    assert "+dataset:" in dbt_project_datasets
    assert "models:" in dbt_project_datasets
