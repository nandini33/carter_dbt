with carter_details as(

  select * from {{ref('carter_details')}}

), 
carter_master_1 as 
(
    select * from {{ source('carter_master','carter_master') }}
),

carter_master as
(
 select distinct a.order_id, a.date_created, a.customer_fees, a.discounts, a.subtotal,a.transaction_amount, 
   a.transaction_id, a.product_details, a.address_id , a.PAYMENT_STATUS , a.address ,a.name, a.c_id , a.is_first_customer
   from carter_details a where a.order_id not in (select order_id from carter_master_1)
   
)
select * from carter_master