{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ default__generate_schema_name(custom_schema_name, node) }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

This tells dbt to use `lz`, `staging`, `dbo` as exact schema names without prepending `DEV_TUSHAR_`.

---

## Do This Right Now

**Step 1** — In VS Code create the macro file:
```
demo_kiewit_dbt/macros/generate_schema_name.sql