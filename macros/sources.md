{% docs customer_profile %}

# HAS CUTOMER DETAILS -- FROM ON-PREMISE

- 1) Address_id is the primary key
- 2) A customer can have multiple addresses but a unique addess_id
- 3) Source data from table "dev.public.customer_details"
- 4) Used along with order_details in model carter_details with common address_id to extract details for carter 
{% enddocs %}

{% docs order_profile %}

# HAS DETAILS OF EACH ORDER MADE IN THE SITE

- 1) order_id is the primary key
- 2) each order_id has a single address_id
- 3) Source data from table "dev.public.order_details"
- 4) Used along with customers in model carter_details with common address_id to extract details for carter
{% enddocs %}


{% docs price_details %}

# HAS PRODUCT AND QUANTITY DETAILS IN SEPARATE COLUMNS AFTER FLATTENING

- 1) order_id is the primary key
- 2) This takes data from table "dev.public.price_details"
- 3) Used along with source data from 'product_details' in model 'product_price' with common product_id  to extract wholesale price for respective product ids.
- 4)this is the table which is being made from the original file to get individual product ids and corresponding quantities
 {% enddocs %}

{% docs delivery_profile %}

# DELIVERY DETAILS FROM SHIPME
- 1) shipme_id is the primary key
- 2) This takes data from table "dev.public.delivery_details"
- 3) Used in model transaction_details along with model carter_details to extract delivery details along with order details 
{% enddocs %}

{% docs product_profile %}

# PRODUCT PROFILE -- FOR ALL PRODUCTS ORDERED
- 1) product_id is the primary key
- 2) This takes data from table "dev.public.product_details"
- 3) Used along with source data from 'price_details' in model 'product_price' with common product_id to extract wholesale price for respective product ids.
{% enddocs %}