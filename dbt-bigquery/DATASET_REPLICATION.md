# BigQuery Dataset Replication Configuration

## Overview

This feature enables native BigQuery dataset replication configuration at the dbt model level, allowing you to specify replica locations and primary replicas without using hooks.

## Configuration Options

### Model-level Configuration

Two new configuration options are available:

- `dataset_replicas`: List of replica location IDs
- `primary_replica`: Primary replica location ID (optional)

### Configuration Methods

#### 1. In-file Configuration Block

```sql
-- models/analytics/revenue_report.sql
{{
  config(
    materialized='table',
    schema='analytics',
    dataset_replicas=['us-east1', 'us-west1', 'europe-west1'],
    primary_replica='us-central1'
  )
}}

SELECT * FROM {{ ref('raw_revenue') }}
```

#### 2. In schema.yml

```yaml
# models/schema.yml
version: 2

models:
  - name: revenue_report
    description: "Critical revenue data requiring high availability"
    config:
      materialized: table
      schema: analytics
      dataset_replicas:
        - us-east1
        - us-west1
        - europe-west1
      primary_replica: us-central1
```

#### 3. In dbt_project.yml (Project-level Defaults)

```yaml
# dbt_project.yml
name: 'my_project'
version: '1.0.0'
config-version: 2

models:
  my_project:
    analytics:
      +schema: analytics
      +dataset_replicas:
        - us-east1
        - us-west1
      +primary_replica: us-central1

    staging:
      +schema: staging
      # No replication for staging
```

## How It Works

### Scenario 1: New Dataset Creation

When you run:
```bash
dbt run --models analytics.*
```

1. dbt parses the model configuration and extracts `dataset_replicas` and `primary_replica`
2. Model specifies `+schema: analytics`
3. Adapter creates the dataset with the configured location
4. Immediately applies replication:
   ```sql
   ALTER SCHEMA `project.analytics` ADD REPLICA `us-east1`;
   ALTER SCHEMA `project.analytics` ADD REPLICA `us-west1`;
   ALTER SCHEMA `project.analytics` SET OPTIONS (default_replica = `us-central1`);
   ```

### Scenario 2: Adding Model to Existing Dataset

When you run:
```bash
dbt run --models analytics.new_model
```

1. Dataset `analytics` already exists
2. Adapter checks current replicas via `INFORMATION_SCHEMA.SCHEMATA_REPLICAS`
3. Compares with model's replication config
4. Config matches → SKIP (no ALTER statements)
5. Creates table

### Scenario 3: Changing Replication Config

If you update your configuration:
```yaml
# dbt_project.yml (updated)
models:
  my_project:
    analytics:
      +dataset_replicas:
        - us-east1
        - us-west1
        - asia-east1  # NEW
```

Then run:
```bash
dbt run
```

1. Config hash changes
2. Calculates diff: `to_add = [asia-east1]`
3. Runs only:
   ```sql
   ALTER SCHEMA `project.analytics` ADD REPLICA `asia-east1`;
   ```

## Example Use Cases

### Multi-Region Replication for High Availability

```yaml
models:
  my_project:
    prod_critical:
      +schema: critical_data
      +dataset_replicas:
        - us-east1
        - us-west1
        - us-central1
        - europe-west1
      +primary_replica: us-central1
      +labels:
        tier: critical
        dr: enabled
```

### Compliance Data with Specific Regions

```yaml
models:
  my_project:
    regulatory:
      +schema: compliance
      +dataset_replicas:
        - us-east1  # Specific compliance region
      +primary_replica: us-east1
      +labels:
        compliance: sox
        security: high
```

### Development Environment (No Replication)

```yaml
models:
  my_project:
    staging:
      +schema: dev
      # No dataset_replicas specified - no replication
      +labels:
        env: dev
```

## Features

✅ **No hooks required** - Native adapter behavior integrated into schema creation flow
✅ **Smart state checking** - Only runs ALTER statements when configuration differs
✅ **First model wins** - First model materialized to a dataset sets replication
✅ **Config changes detected** - Automatically updates on config change
✅ **DDL-only** - Uses ALTER SCHEMA statements, no bq CLI dependency
✅ **Works with all materializations** - Table, view, incremental, etc.
✅ **Project-level defaults** - Set defaults in dbt_project.yml like other configs
✅ **Per-model override** - Override at model or directory level

## Technical Details

### Implementation

The replication feature is implemented through:

1. **BigqueryConfig**: New fields `dataset_replicas` and `primary_replica`
2. **Macro**: `bigquery__create_schema` extracts config and calls adapter method
3. **Adapter Method**: `create_dataset_with_replication()` handles dataset creation and replication
4. **Connection Manager**: Updated `create_dataset()` to accept replication parameters
5. **Replication Logic**: Functions in `dataset.py`:
   - `get_dataset_replication_config()` - Queries `INFORMATION_SCHEMA.SCHEMATA_REPLICAS`
   - `needs_replication_update()` - Compares current vs desired state
   - `apply_dataset_replication()` - Executes DDL to add/remove replicas

### SQL Statements Used

```sql
-- Query current replicas
SELECT replica_location, is_primary_replica
FROM `project.dataset`.INFORMATION_SCHEMA.SCHEMATA_REPLICAS
WHERE schema_name = 'dataset';

-- Add replica
ALTER SCHEMA `project.dataset` ADD REPLICA `location`;

-- Drop replica
ALTER SCHEMA `project.dataset` DROP REPLICA `location`;

-- Set primary replica
ALTER SCHEMA `project.dataset` SET OPTIONS (default_replica = `location`);
```

## Requirements

- BigQuery dataset must support replication (regional or multi-regional datasets)
- User must have permissions to:
  - Create datasets
  - Alter datasets
  - Query `INFORMATION_SCHEMA`

## Notes

- Replication configuration is applied at the **dataset level**, not the table level
- The first model to materialize into a dataset will establish the replication configuration
- Subsequent models in the same dataset will use the existing replication configuration
- To change replication for an existing dataset, update the config and re-run dbt
