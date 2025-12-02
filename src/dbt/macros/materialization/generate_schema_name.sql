{#-
    Custom schema generation macro.

    This overrides dbt Core `generate_schema_name` behavior so that
    models are materialized into different schemas depending on the target
    environment.

    Rules:
    - In the `prod` target: models use their defined schema directly 
      (e.g. `il`, `ol`, `bl`).
    - In non-prod targets (e.g. `dev`): the target schema is prefixed 
      with the custom schema name (e.g. `dev_il`, `dev_ol`, `dev_bl`).

    If a model does not specify a custom schema, the target’s default schema
    is used instead.

    The logic for the table/view name itself is covered by generate_alias_name.

-#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
    {# Case for when no custom schema is set, only likely to happen if running models from package without assigned schema. #}
        {{ default_schema }}

    {%- elif target.name == 'prod' -%}
    {# Case for prod, materialise into non-prefixed "il", "ol" .etc. #}
        {{ custom_schema_name }}

    {%- else -%}
    {# Case for prod, materialise into non-prefixed "il", "ol" .etc. #}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
