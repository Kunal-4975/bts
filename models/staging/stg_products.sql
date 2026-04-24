select 
    doc:productId::string as product_id,
    doc:sku::string as sku,
    doc:name::string as product_name,
    doc:category:l1::string as category_l1,
    doc:category:l2::string as category_l2,
    doc:active::boolean as is_active,
    to_timestamp(doc:audit:updatedAt:"$date"::STRING) AS product_updated_at
from {{ source('raw', 'products') }}
;