{#
  ClickHouse has no separate "database.schema" hierarchy, so dbt's default behaviour
  of prefixing a custom schema with the target schema would create names like
  "analytics_staging". Return the custom schema unchanged so `+schema: staging`
  lands in the `staging` database and `+schema: marts` in `marts`.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
