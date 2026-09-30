-- Custom reconciliation test: an order's stored total must equal the sum of its
-- lines. This is the kind of check a generic not_null/unique test cannot express,
-- and it catches a broken join or a double-counted line in upstream generation.
--
-- Fails (returns rows) when the two differ by more than a cent.

select
    o.order_id,
    o.total_amount,
    sum(i.line_amount) as items_total
from {{ ref('stg_orders') }} as o
inner join {{ ref('stg_order_items') }} as i
    on o.order_id = i.order_id
group by o.order_id, o.total_amount
having abs(toFloat64(o.total_amount) - toFloat64(sum(i.line_amount))) > 0.01
