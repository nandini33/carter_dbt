{{ config(
    full_refresh = true
) }}
with price_details as(
    select * from {{ source('price_details','price_details') }}
),

product_details as(

    select * from {{ source('product_profile','product_details') }}
),

product_price as
(
    select DISTINCT a.order_id, a.product_id1, a.quantity1, 
    (select wholesale_price from product_details where product_id = a.product_id1) as wp1,
    a.product_id2, a.quantity2,
    (select wholesale_price from product_details where product_id = a.product_id2) as wp2 ,
    a.product_id3, a.quantity3, 
    (select wholesale_price from product_details where product_id = a.product_id3 ) as wp3 ,
    a.product_id4, a.quantity4,
    (select wholesale_price from product_details where product_id = a.product_id4) as wp4 
    from price_details a, product_details b
)

select * from product_price