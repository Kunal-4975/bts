select 
    doc:customerId::string as customer_id,
    doc:region::string as region,
    doc:name:first::string as first_name,
    doc:name:last::string as last_name,
    doc:contacts:emails[0]::string as email,
    doc:loyalty:tier::string as loyalty_tier,
    to_timestamp(doc:audit:updatedAt:"$date"::STRING) AS customer_updated_at,
    doc:isDeleted::boolean as is_deleted
from {{ source('raw', 'customers') }}
;