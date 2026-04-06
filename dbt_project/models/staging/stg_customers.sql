-- Staging model: standardize and clean raw customers data.
-- Why normalize names here: downstream models should not need to handle
-- mixed-case or whitespace variations in customer names.

WITH source_customers AS (
    SELECT
        customer_id
        , LOWER(TRIM(email)) AS email
        , INITCAP(TRIM(first_name)) AS first_name
        , INITCAP(TRIM(last_name)) AS last_name
        , UPPER(TRIM(country)) AS country
        , created_at
        , updated_at
    FROM {{ source('public', 'customers') }}
    WHERE customer_id IS NOT NULL
)

, deduplicated AS (
    SELECT
        customer_id
        , email
        , first_name
        , last_name
        , country
        , created_at
        , updated_at
        , ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY updated_at DESC
        ) AS row_num
    FROM source_customers
)

SELECT
    customer_id
    , email
    , first_name
    , last_name
    , country
    , created_at
    , updated_at
FROM deduplicated
WHERE row_num = 1
