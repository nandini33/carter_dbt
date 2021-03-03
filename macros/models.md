{% docs transaction_details %}

# AN INTERMEDIATE MODEL FOR EXTRACTING DELIVERY AND ORDER DETAILS
- 1) Takes transaction details from model 'carter_details'
- 2) Source data from source 'delivery_profile' having all delivery details for the order made
- 3) Joins the two based on order_id 
{% enddocs %}

{% docs final_details %}

# FINAL MODEL FOR CARTER HAVING DELIVERY AND TRANSACTION DETAILS
- 1) Takes details to calculate total price from model 'product_price'
- 2) Takes remaining details from model 'transaction_details'
- 3) Joins these two based on order_id to get all the order details 
{% enddocs %}

{% docs product_price %}

# AN INTERMEDIATE MODEL TO EXTRACT DETAILS TO CALCULATE TOTAL PRICE FOR EACH ORDER
- 1) Takes product quantity details for each order from source 'price_details'
- 2) Takes price of each product from source 'product_profile'
- 3) Flattens the json in the table 'price_details' and makes different columns of data to be used directly
- 4) Details matched to all orders using order_id 
{% enddocs %}

{% docs carter_details %}
# TO EXTRACT ALL DETAILS AT CARTER FOR EACH UNIQUE ADDRESS_ID
- 1) uses source 'customer_profile' and 'order_profile'
- 2) joins the two based on address_id 
{% enddocs %}