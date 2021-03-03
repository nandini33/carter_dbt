{{ config(
    full_refresh = true
) }}

with transaction_details as(

  select * from {{ref('transaction_details')}}

), 

product_price as(

  select * from {{ref('product_price')}}

), 

final_records as(
    select a.order_id, a.customer_fees, a.discounts,a.PAYMENT_STATUS, 
    a.TAXES, a.is_first_customer,
    case 
    when (b.wp3 is not null and b.wp4 is not null and b.wp3 is not null)then (b.quantity1*b.wp1 + b.quantity2*b.wp2 + b.quantity3*b.wp3 + b.quantity4*b.wp4) 
    when (b.wp4 is null and b.wp3 is not null) then (b.quantity1*b.wp1 + b.quantity2*b.wp2 + b.quantity3*b.wp3)
    when (b.wp3 is null and b.wp4 is null) then (b.quantity1*b.wp1 + b.quantity2*b.wp2)
    else (b.quantity1*b.wp1)
    END as wholesale_cost ,
    (a.subtotal- wholesale_cost - (discounts*subtotal/100)) as margin_earned ,
    a.address_id, a.address ,a.name , a.shipme_id, a.delivery_exec , a.t_of_dipatch , a.t_of_pickup ,
    DATEDIFF(HOURS,a.t_of_dipatch ,a.t_of_pickup) as TIMEDIFFERENCE_HOURS,
    {{ dbt_utils.surrogate_key (['a.order_id', 'shipme_id','address_id']) }} as my_key
    from transaction_details a,product_price b
    where a.order_id = b.order_id
)

select * from final_records