{{ config(
    full_refresh = true
) }}

with carter_details as(

  select * from {{ref('carter_master')}}

), 

delivery_details as 
(
    select * from {{ source('delivery_profile','delivery_details') }}
),

transaction_details as
(
select a.order_id, a.customer_fees, a.discounts, a.subtotal, a.is_first_customer,
 a.transaction_amount - a.subtotal as TAXES ,a.transaction_amount, a.transaction_id,a.PAYMENT_STATUS,
   a.address_id, a.address ,a.name , c.shipme_id, c.delivery_exec , c.t_of_dipatch , c.t_of_pickup 
   from carter_details a, delivery_details c 
   where a.order_id = c.order_id

)

select * from transaction_details

