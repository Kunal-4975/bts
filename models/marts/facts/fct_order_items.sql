select 
    o.order_id,
    region,
    i.value:itemId::string as item_id,
    i.value:product:productId::string as product_id,
    i.value:qty::int as quantity,
    i.value:pricing:unitPrice::float as unit_price,
    i.value:pricing:currency::string as currency,
    quantity * unit_price as line_gross,
    coalesce(i.value:pricing:discounts[0]:value::float / 100 * line_gross, 0) as line_discount,
    line_gross - line_discount as line_net
from {{ ref('stg_orders') }} o,
lateral flatten(input => parse_json(o.order_items)) i
