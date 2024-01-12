CREATE TABLE tb_users_agg as 
SELECT first_sesssion_date
     , session_qty as session_number_max
     , CASE WHEN session_number_1st_order < 5 THEN CAST (session_number_1st_order AS STRING) 
           ELSE '5+' END as session_number_1st_order

    , SUM(1) as users
    , SUM(CASE WHEN order_qty > 0 then 1 else 0 end) as users_with_order    
    , SUM(session_qty) as session_qty
    , SUM(order_qty) as order_qty


FROM tb_users
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;