{% macro databricks__get_create_materialized_view_as_sql(relation, sql) -%}
  {# Column masks are supported in DBSQL, but not yet wired up to the adapter. Return a helpful error until supported. #}
  {% if column_mask_exists() %}
    {% do exceptions.raise_compiler_error("Column masks are not yet supported for materialized views.") %}
  {% endif %}
  {%- set materialized_view = adapter.get_config_from_model(config.model) -%}
  {%- set partition_by = materialized_view.config["partition_by"].partition_by -%}
  {%- set tblproperties = materialized_view.config["tblproperties"].tblproperties -%}
  {%- set comment = materialized_view.config["comment"].comment -%}
  {%- set refresh = materialized_view.config["refresh"] -%}

  create materialized view {{ relation.render() }}
    {%- if config.persist_column_docs() -%}
      {%- set model_columns = model.columns -%}
      {%- set query_columns = get_columns_in_query(sql) -%}
      {%- if query_columns %}
    (
      {{ get_persist_docs_column_list(model_columns, query_columns) }}
    )
      {%- endif -%}
    {%- endif %}
    {{ get_create_sql_partition_by(partition_by) }}
    {{ get_create_sql_comment(comment) }}
    {{ get_create_sql_tblproperties(tblproperties) }}
    {{ get_create_sql_refresh_schedule(refresh.cron, refresh.time_zone_value) }}
  as
    {{ sql }}
{% endmacro %}
