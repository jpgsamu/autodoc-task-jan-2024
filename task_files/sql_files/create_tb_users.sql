CREATE TABLE tb_users as 
with aux_1st_purchase as (
SELECT 
       -- Helps us mark when is the first order
       user_id
     , MIN(session_start) as session_start_1st_order
     , MIN(session_number) as session_number_1st_order

FROM tb_sessions

WHERE has_event_order = 1
GROUP BY 1)
--

SELECT 
        -- User general information
      main.user_id
    , SUM(1) as session_qty
    , MIN(session_date) as first_sesssion_date
    , MAX(session_date) as last_sesssion_date
    , SUM(order_qty) as order_qty
    , AVG(days_from_last_session) as avg_time_between_sessions
 
    -- Information of the 1st order, we'll aggregate since it's repeated on every row for the sam user_id
    , MAX(session_start_1st_order) as session_start_1st_order
    , MAX(session_number_1st_order) as session_number_1st_order

FROM tb_sessions main
LEFT JOIN aux_1st_purchase aux ON main.user_id = aux.user_id

GROUP BY 1
;