
{{ config(
    full_refresh = true
) }}

with customer_details as 
(
    select * from {{ source('customer_profile', 'customer_details') }}
),

order_details as 
(
    select * from {{ source('order_profile','order_details') }}
),

carter as
(
   select a.order_id, a.date_created, a.customer_fees, a.discounts, a.subtotal,a.transaction_amount, 
   a.transaction_id, a.product_details, a.address_id ,
    (case
    when (a.transaction_id is not null) then 'payment_done'
    else 'pending'
    END) AS PAYMENT_STATUS , b.address, b.c_id ,b.name ,
    count(b.c_id) over(partition by b.c_id) as cid_count ,
    case 
    when cid_count > 1 then 'false'
    else 'true'
    end as is_first_customer
   from order_details a,customer_details b
   where a.address_id = b.address_id 
)

select * from carter