with orders as (
    select 
        ORDER_ID,
        CUSTOMER_ID,
        ORDER_DATE,
        ORDER_STATUS
    from ANALYTICS.DBT_SZAID.STG_JAFFLE_SHOP__ORDERS
),

payments as (
    select 
        ORDERID,
        STATUS,
        AMOUNT
    from ANALYTICS.DBT_SZAID.STG_STRIPE__PAYMENT
),

order_payments as (
    select
        ORDERID,
        sum(case when STATUS = 'success' then AMOUNT end) as amount
    from payments
    group by ORDERID
),

final as (
    select
        orders.ORDER_ID,
        orders.CUSTOMER_ID,
        orders.ORDER_DATE,
        coalesce(order_payments.amount, 0) as amount
    from orders
    left join order_payments on orders.ORDER_ID = order_payments.ORDERID
)

select * from final
