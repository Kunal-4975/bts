select 
    customer_id,
    region,
    concat(first_name, ' ', last_name) as customer_name,
    email,
    loyalty_tier,
    customer_updated_at,
    case when loyalty_tier = 'Gold' then 'VIP' else 'Standard' end as customer_segment
from {{ ref('stg_customers') }}
where is_deleted = false
;
