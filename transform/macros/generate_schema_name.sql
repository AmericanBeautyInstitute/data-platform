{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        Uses the custom schema name verbatim (e.g. "staging", "marts") rather than
        dbt's default of prefixing it with the target schema.
    -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
