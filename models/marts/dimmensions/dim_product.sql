select distinct
    product_id,
    sku,
    product_name,
    category_l1,
    category_l2,
    is_active
from {{ ref('stg_products') }}
;