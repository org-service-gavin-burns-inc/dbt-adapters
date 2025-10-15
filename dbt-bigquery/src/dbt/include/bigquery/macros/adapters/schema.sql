{% macro bigquery__create_schema(relation) -%}
  {%- set dataset_replicas = config.get('dataset_replicas') -%}
  {%- set primary_replica = config.get('primary_replica') -%}
  
  {%- if dataset_replicas -%}
    {{ log("Configuring dataset " ~ relation.schema ~ " with replicas: " ~ dataset_replicas | join(', '), info=True) }}
    {%- if primary_replica -%}
      {{ log("  Primary replica: " ~ primary_replica, info=True) }}
    {%- endif -%}
  {%- endif -%}
  
  {# Call adapter's create_dataset_with_replication to handle both schema creation and replication #}
  {% do adapter.create_dataset_with_replication(relation, dataset_replicas, primary_replica) %}
  
{% endmacro %}
