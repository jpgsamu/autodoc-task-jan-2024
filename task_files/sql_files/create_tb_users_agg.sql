CREATE TABLE tb_users_agg as 

with tb_full as (
SELECT first_sesssion_date
     , CASE WHEN session_number_1st_order < 5 THEN CAST (session_number_1st_order AS STRING) 
            WHEN session_number_1st_order >= 5 THEN '5+'
            ELSE 'No Order' END as session_number_1st_order
    , SUM(1) as users
    , SUM(CASE WHEN order_qty > 0 THEN 1 ELSE 0 END) as users_with_order    
    , SUM(session_qty) as session_qty
    , SUM(order_qty) as order_qty

FROM tb_users

GROUP BY 1, 2)
--

, tb_aux as (
SELECT first_sesssion_date
    , SUM(1) as users
    , SUM(CASE WHEN order_qty > 0 THEN 1 ELSE 0 END) as users_with_order    

FROM tb_users

GROUP BY 1)
--

SELECT  tb_full.*
      , tb_aux.users as pivot_users_aux
      , tb_aux.users_with_order as pivot_users_with_order_aux

FROM tb_full
LEFT JOIN tb_aux ON tb_full.first_sesssion_date = tb_aux.first_sesssion_date
;