CREATE TABLE tb_users_agg as 

with tb_full as (
SELECT first_sesssion_date
     , session_qty as session_number_max
     , CASE WHEN session_number_1st_order < 5 THEN CAST (session_number_1st_order AS STRING) 
            WHEN session_number_1st_order >= 5 THEN '5+'
            ELSE 'No Order' END as session_number_1st_order

    , SUM(1) as users
    , SUM(CASE WHEN order_qty > 0 THEN 1 ELSE 0 END) as users_with_order    
    , SUM(session_qty) as session_qty
    , SUM(order_qty) as order_qty

FROM tb_users

GROUP BY 1, 2, 3)
--

, tb_aux as (
SELECT first_sesssion_date
     , session_qty as session_number_max
    , SUM(1) as users
 
FROM tb_users

GROUP BY 1, 2)
--

SELECT  tb_full.*
      , tb_aux.users as pivot_users_aux

FROM tb_full
LEFT JOIN tb_aux ON tb_full.first_sesssion_date = tb_aux.first_sesssion_date
                 AND tb_full.session_number_max = tb_aux.session_number_max
;
