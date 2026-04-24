{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id'
) }}

with latest_orders as (
    select 
		*,
		row_number() over (
		   partition by order_id 
		   order by order_updated_at desc
		) as rn
    from {{ ref('stg_orders') }}
    where is_deleted = false
    
	{% if is_incremental() %}
    and order_updated_at > (
        select coalesce(max(order_updated_at), '1900-01-01') from {{ this }}
    )
    {% endif %}
),

status_ranked as (
    select
        lo.order_id,
        sh.value:status::string as status,
        sh.value:ts::timestamp as ts,
        row_number() over (
            partition by lo.order_id 
            order by sh.value:ts desc
        ) as rn
    from latest_orders lo,
	lateral flatten(input => lo.status_history) sh
    where lo.rn = 1
),

latest_status as (
    select order_id, status as current_status
    from status_ranked
    where rn = 1
),

final as (
    select 
        lo.order_id,
        lo.region,
        lo.customer_id,
        lo.order_created_at,
        lo.order_updated_at,
        -- Latest status
        ls.current_status,
		-- Gross amount
		sum(i.value:pricing:unitPrice::float * i.value:qty::int) as gross_amount,
		-- Discounts
		sum(coalesce(i.value:discounts[0]:value::float, 0) * i.value:pricing:unitPrice::float * i.value:qty::int / 100 ) as discount_amount,
		gross_amount - discount_amount as net_amount
    from latest_orders lo
	join latest_status ls 
	on lo.order_id = ls.order_id,
    lateral flatten(input => lo.order_items) i
    where lo.rn = 1
    group by 1,2,3,4,5,6
)

select * from final
