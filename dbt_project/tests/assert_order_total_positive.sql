-- Custom test: ensure no orders have negative total amounts.
-- Why a dedicated test: the min_value quality rule catches this at the
-- governance layer, but having it in dbt ensures the check runs during
-- every dbt build regardless of governance pipeline scheduling.

SELECT
    order_id
    , total_amount
FROM {{ ref('stg_orders') }}
WHERE total_amount < 0
