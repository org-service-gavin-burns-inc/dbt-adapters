import pytest
import os
import json
from dbt.adapters.bigquery.credentials import _is_base64, _base64_to_string

# Import the functional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# dbt_project.yml configuration for dataset-related tests
dbt_project_datasets_yml = """
name: test_datasets
version: '1.0'
config-version: 2

profile: test

# Top-level dataset configurations
datasets:
  analytics:
    location: 'US'
    replication:
      enabled: true
      replicas:
        - us-east1
        - us-west1
      primary_location: us-central1
    labels:
      env: prod
      tier: critical
  
  staging:
    location: 'US'
    labels:
      env: dev

models:
  test_datasets:
    analytics:
      +schema: analytics  # Uses datasets.analytics config
    staging:
      +schema: staging    # Uses datasets.staging config
"""


@pytest.fixture(scope="class")
def dbt_project_datasets():
    """Fixture providing dbt_project.yml configuration for dataset testing with top-level datasets."""
    return dbt_project_datasets_yml


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="oauth", type=str)


@pytest.fixture(scope="class")
def dbt_profile_target(request):
    profile_type = request.config.getoption("--profile")
    if profile_type == "oauth":
        target = oauth_target()
    elif profile_type == "service_account":
        target = service_account_target()
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")
    return target


def oauth_target():
    return {
        "type": "bigquery",
        "method": "oauth",
        "threads": 4,
        "job_retries": 2,
        "compute_region": os.getenv("COMPUTE_REGION") or os.getenv("DATAPROC_REGION"),
        "dataproc_cluster_name": os.getenv("DATAPROC_CLUSTER_NAME"),
        "gcs_bucket": os.getenv("GCS_BUCKET"),
    }


def service_account_target():
    credentials_json_str = os.getenv("BIGQUERY_TEST_SERVICE_ACCOUNT_JSON").replace("'", "")
    if _is_base64(credentials_json_str):
        credentials_json_str = _base64_to_string(credentials_json_str)
    credentials = json.loads(credentials_json_str)
    project_id = credentials.get("project_id")
    return {
        "type": "bigquery",
        "method": "service-account-json",
        "threads": 4,
        "job_retries": 2,
        "project": project_id,
        "keyfile_json": credentials,
        # following 3 for python model
        "compute_region": os.getenv("COMPUTE_REGION") or os.getenv("DATAPROC_REGION"),
        "dataproc_cluster_name": os.getenv(
            "DATAPROC_CLUSTER_NAME"
        ),  # only needed for cluster submission method
        "gcs_bucket": os.getenv("GCS_BUCKET"),
    }
