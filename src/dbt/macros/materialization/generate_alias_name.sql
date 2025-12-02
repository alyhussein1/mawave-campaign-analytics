{#-
    Custom alias generation macro.

    This overrides dbt Core `generate_alias_name` behavior so that the
    final table/view name is derived from the model file name by
    removing the prefix before the first underscore.

    Example:
      file name:  il_campaign_performance
      table name created in BQ: campaign_performance

    Behaviour:
    - If a custom alias is explicitly defined via `alias=`, it is used.
    - Otherwise, everything after the first underscore of the model file
      name becomes the alias.
    - As a fallback (no underscore found), the model file name is used
      as-is.

    Schema naming is controlled separately by `generate_schema_name`.
-#}

{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {% set split_node_name = node.name.split('_') %}

    {%- if custom_alias_name is not none -%}
    {#Case priorises using a manually set alias over general rules.#}
        {{ custom_alias_name | trim }}

    {%- elif split_node_name[1] is not none -%}
    {#Case uses everything after first underscore as the alias.#}
        {{ split_node_name[1:]|join("_") }}

    {%- else -%}
    {#Failsafe case, the name of the file itself.#}
        {{ node.name }}

    {%- endif -%}
{%- endmacro %}
