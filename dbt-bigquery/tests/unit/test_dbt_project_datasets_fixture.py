"""Test the dbt_project_datasets fixture."""

import pytest


def test_dbt_project_datasets_fixture_exists(dbt_project_datasets):
    """Test that the dbt_project_datasets fixture returns valid YAML."""
    assert dbt_project_datasets is not None
    assert isinstance(dbt_project_datasets, str)
    assert "name:" in dbt_project_datasets
    assert "test_datasets" in dbt_project_datasets
    assert "config-version:" in dbt_project_datasets


def test_dbt_project_datasets_fixture_contains_datasets_config(dbt_project_datasets):
    """Test that the fixture contains top-level datasets configuration."""
    assert "datasets:" in dbt_project_datasets
    assert "analytics:" in dbt_project_datasets
    assert "staging:" in dbt_project_datasets
    assert "replication:" in dbt_project_datasets
    assert "replicas:" in dbt_project_datasets
    assert "labels:" in dbt_project_datasets


def test_dbt_project_datasets_fixture_contains_model_references(dbt_project_datasets):
    """Test that the fixture contains model references to datasets."""
    assert "models:" in dbt_project_datasets
    assert "+schema: analytics" in dbt_project_datasets
    assert "+schema: staging" in dbt_project_datasets
