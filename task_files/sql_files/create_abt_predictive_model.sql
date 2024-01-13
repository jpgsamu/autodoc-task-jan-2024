CREATE TABLE abt as 
with tb_sessions as (
SELECT session as session_id
     , user as user_id

     , MIN(DATE(event_date)) as session_date
     , MIN(event_date) as session_start
     , MAX(event_date) as session_end
     , ROUND((JULIANDAY(MAX(event_date)) - JULIANDAY(MIN(event_date))) * 24 * 60 * 60) as session_duration_sec
       
     , SUM(CASE WHEN event_type = "add_to_cart" THEN 1 ELSE 0 END) as add_to_cart_qty
     , MAX(CASE WHEN event_type = "order" THEN 1 ELSE 0 END) as has_order

      , SUM(CASE WHEN event_type = "page_view" AND page_type = "product_page" THEN 1 ELSE 0 END) as page_pdp_qty
      , SUM(CASE WHEN event_type = "page_view" AND page_type = "listing_page" THEN 1 ELSE 0 END) as page_plp_qty
      , SUM(CASE WHEN event_type = "page_view" AND page_type = "search_listing_page" THEN 1 ELSE 0 END) as page_search_plp_qty

      , SUM(1) as events_qty

FROM data_set_da_test

GROUP BY 1, 2)
--

, tb_sessions_rn as (
SELECT *
     , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_start) as session_number

FROM tb_sessions)
--

, tb_1st_order as (
SELECT  user_id
      , MIN(session_number) as session_number_1st_order

FROM tb_sessions_rn

WHERE has_order = 1

GROUP BY 1)
--

, tb_analysis as (
SELECT main.*
     , CASE WHEN main.session_number < session_number_1st_order THEN 'INACTIVE' ELSE 'ACTIVE' END as status
     , CASE WHEN LEAD(main.session_number) OVER (PARTITION BY main.user_id ORDER BY main.session_start) = session_number_1st_order THEN 1 ELSE 0 END as is_next_session_activation
     , aux.session_start as user_first_timestamp
     , JULIANDAY(main.session_end) - JULIANDAY(aux.session_start) as days_in_base

FROM tb_sessions_rn main
INNER JOIN tb_1st_order ON main.user_id = tb_1st_order.user_id
INNER JOIN tb_sessions_rn aux ON main.user_id = aux.user_id AND aux.session_number = 1)
--

SELECT  user_id
      , session_id

      , days_in_base
      , session_number as session_qty_acc
      , SUM(add_to_cart_qty) OVER (PARTITION BY user_id ORDER BY session_start) as add_to_cart_qty_acc

      , SUM(page_pdp_qty) OVER (PARTITION BY user_id ORDER BY session_start) as page_pdp_qty_acc
      , SUM(page_plp_qty) OVER (PARTITION BY user_id ORDER BY session_start) as page_plp_qty_acc
      , SUM(page_search_plp_qty) OVER (PARTITION BY user_id ORDER BY session_start) as page_search_plp_qty_acc
    
      , SUM(CASE WHEN events_qty = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY session_start) as bounce_sessions_qty_acc
      , SUM(session_duration_sec) OVER (PARTITION BY user_id ORDER BY session_start) as navigation_time_acc

      , is_next_session_activation

FROM tb_analysis

WHERE status = 'INACTIVE'
;