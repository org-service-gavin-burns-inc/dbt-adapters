# Example dbt Project Configuration with Dataset Replication

This directory contains example configurations demonstrating how to use the BigQuery dataset replication feature.

## Project Structure

```
my_dbt_project/
├── dbt_project.yml          # Project-level configuration
├── models/
│   ├── analytics/           # Analytics models (replicated)
│   │   ├── schema.yml
│   │   └── revenue_report.sql
│   ├── staging/             # Staging models (no replication)
│   │   ├── schema.yml
│   │   └── stg_orders.sql
│   └── critical/            # Critical models (multi-region)
│       ├── schema.yml
│       └── critical_metrics.sql
└── README.md
```

## Example Configurations

### 1. Project-Level Configuration (dbt_project.yml)

```yaml
name: 'my_project'
version: '1.0.0'
config-version: 2

models:
  my_project:
    # Analytics models - 2 replicas
    analytics:
      +schema: analytics
      +dataset_replicas:
        - us-east1
        - us-west1
      +primary_replica: us-east1
      +materialized: table
    
    # Staging models - no replication
    staging:
      +schema: staging
      +materialized: view
    
    # Critical models - multi-region replication
    critical:
      +schema: critical_data
      +dataset_replicas:
        - us-east1
        - us-west1
        - us-central1
        - europe-west1
      +primary_replica: us-central1
      +materialized: table
```

### 2. Schema-Level Configuration (models/analytics/schema.yml)

```yaml
version: 2

models:
  - name: revenue_report
    description: "Daily revenue metrics requiring high availability"
    config:
      materialized: table
      schema: analytics
      dataset_replicas:
        - us-east1
        - us-west1
      primary_replica: us-east1
    columns:
      - name: report_date
        description: "Date of the revenue report"
      - name: total_revenue
        description: "Total revenue for the day"
```

### 3. Model-Level Configuration (models/analytics/revenue_report.sql)

```sql
{{
  config(
    materialized='table',
    schema='analytics',
    dataset_replicas=['us-east1', 'us-west1'],
    primary_replica='us-east1',
    partition_by={
      "field": "report_date",
      "data_type": "date"
    },
    cluster_by=['product_category']
  )
}}

-- Your SQL query here
SELECT
  CURRENT_DATE() as report_date,
  product_category,
  SUM(revenue) as total_revenue,
  COUNT(DISTINCT customer_id) as unique_customers
FROM {{ ref('stg_orders') }}
GROUP BY 1, 2
```

### 4. Different Replication per Environment

```yaml
# dbt_project.yml with environment-specific configuration

models:
  my_project:
    analytics:
      +schema: analytics
      # Production gets replicas
      +dataset_replicas: "{{ var('dataset_replicas', []) if target.name == 'prod' else [] }}"
      +primary_replica: "{{ var('primary_replica', none) if target.name == 'prod' else none }}"
```

Then use profiles.yml or dbt Cloud variables:

```yaml
# profiles.yml
my_project:
  outputs:
    dev:
      type: bigquery
      project: my-dev-project
      dataset: dev_analytics
      # No replication for dev
    
    prod:
      type: bigquery
      project: my-prod-project
      dataset: analytics
      # Variables for prod
  
  target: dev
```

And set variables for prod:
```bash
dbt run --target prod --vars '{"dataset_replicas": ["us-east1", "us-west1"], "primary_replica": "us-east1"}'
```

## Running the Examples

### Initial Run (Creates dataset with replication)
```bash
# Creates analytics dataset with replicas in us-east1 and us-west1
dbt run --models analytics.*
```

### Subsequent Runs (Replication already configured)
```bash
# Dataset already has replication - skips ALTER statements
dbt run --models analytics.new_model
```

### Changing Replication (Update configuration)
```yaml
# Update dbt_project.yml to add asia-east1
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
# Detects change and adds asia-east1 replica
dbt run --models analytics.*
```

## Expected Output

When running with replication configured, you'll see:
```
Configuring dataset analytics with replicas: us-east1, us-west1
  Primary replica: us-east1
Configuring replication for dataset my-project.analytics
Adding replica: us-east1
Adding replica: us-west1
Setting primary replica: us-east1
```

On subsequent runs with no changes:
```
Dataset my-project.analytics replication already configured correctly
```

## Tips

1. **First Model Wins**: The first model materialized to a dataset sets its replication
2. **Consistent Configuration**: Ensure all models writing to the same dataset use the same replication config
3. **Environment Separation**: Use different datasets for dev/staging/prod
4. **Cost Consideration**: Replication incurs additional storage costs
5. **Region Selection**: Choose replica locations based on your user geography

## Troubleshooting

### Issue: Replicas not being added
- Verify you have permissions to alter datasets
- Check that the dataset location supports replication
- Ensure replica locations are valid BigQuery regions

### Issue: Primary replica not being set
- Verify the primary location is in the list of replicas
- Check BigQuery documentation for primary replica requirements

### Issue: "Already exists" errors
- This is normal - the adapter handles these gracefully
- Check logs to ensure replicas are being created

## Learn More

See [DATASET_REPLICATION.md](../DATASET_REPLICATION.md) for full documentation.
