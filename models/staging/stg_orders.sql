select 
	doc:orderId::string as order_id,
	doc:region::string as region,
	doc:customer:customerId::string as customer_id,
	doc:statusHistory as status_history,
	doc:items as order_items,
	to_timestamp(doc:audit:createdAt::STRING) AS order_created_at,
	to_timestamp(doc:audit:updatedAt::STRING) AS order_updated_at,
	doc:isDeleted::boolean as is_deleted
from {{ source('raw', 'orders') }}
where doc:orderId is not null
;
