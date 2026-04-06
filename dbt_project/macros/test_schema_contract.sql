-- Macro: validate that a model's columns match the data contract definition.
-- Why a macro: enforces contract compliance within the dbt build process,
-- catching schema drift before data reaches downstream consumers.
-- Usage: {{ test_schema_contract(model, expected_columns=['id', 'name', 'created_at']) }}

{% macro test_schema_contract(model, expected_columns) %}

WITH actual_columns AS (
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = '{{ model.schema }}'
      AND table_name = '{{ model.name }}'
)

, expected AS (
    {% for col in expected_columns %}
    SELECT '{{ col }}' AS column_name
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

, missing_columns AS (
    SELECT e.column_name
    FROM expected e
    LEFT JOIN actual_columns a
        ON e.column_name = a.column_name
    WHERE a.column_name IS NULL
)

SELECT
    column_name AS missing_column
FROM missing_columns

{% endmacro %}
