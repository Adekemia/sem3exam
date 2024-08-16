-- models/dataset/orders_by_state.sql
/*
SELECT 
    c.customer_state,
    COUNT(o.order_id) AS number_of_orders
FROM 
    dataset.orders o
JOIN 
    dataset.customers c 
ON 
    o.customer_id = c.customer_id
GROUP BY 
    c.customer_state
ORDER BY 
    number_of_orders DESC
