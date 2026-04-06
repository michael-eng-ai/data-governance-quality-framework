-- Macro: generic freshness SLA test for any model.
-- Why a macro: enables reusable freshness checks across all models without
-- duplicating the timestamp comparison logic in every test file.
-- Usage: {{ test_freshness_sla(model, column='updated_at', max_delay_minutes=120) }}

{% macro test_freshness_sla(model, column='updated_at', max_delay_minutes=120) %}

WITH freshness_check AS (
    SELECT
        MAX({{ column }}) AS last_updated
        , CURRENT_TIMESTAMP AS check_time
        , EXTRACT(EPOCH FROM (
            CURRENT_TIMESTAMP - MAX({{ column }})
        )) / 60.0 AS delay_minutes
    FROM {{ model }}
)

SELECT
    last_updated
    , check_time
    , delay_minutes
    , {{ max_delay_minutes }} AS max_allowed_minutes
FROM freshness_check
WHERE delay_minutes > {{ max_delay_minutes }}
   OR last_updated IS NULL

{% endmacro %}
