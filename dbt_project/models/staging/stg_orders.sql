-- Staging model: standardize and clean raw orders data.
-- Why a staging layer: isolates source-specific transformations so downstream
-- models work with a consistent interface regardless of source system changes.

WITH source_orders AS (
    SELECT
        order_id
        , customer_id
        , order_date
        , total_amount
        , LOWER(TRIM(status)) AS status
        , created_at
        , updated_at
    FROM {{ source('public', 'orders') }}
    WHERE order_id IS NOT NULL
)

, deduplicated AS (
    SELECT
        order_id
        , customer_id
        , order_date
        , total_amount
        , status
        , created_at
        , updated_at
        , ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY updated_at DESC
        ) AS row_num
    FROM source_orders
)

SELECT
    order_id
    , customer_id
    , order_date
    , total_amount
    , status
    , created_at
    , updated_at
FROM deduplicated
WHERE row_num = 1
