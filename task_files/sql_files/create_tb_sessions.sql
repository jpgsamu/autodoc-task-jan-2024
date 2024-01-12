CREATE TABLE tb_sessions as 
with general_session_info as (
SELECT 
       -- General information
       session as session_id
     , user as user_id
     , MIN(DATE(event_date)) as session_date
     , MIN(event_date) as session_start
     , MAX(event_date) as session_end
     , ROUND((JULIANDAY(MAX(event_date)) - JULIANDAY(MIN(event_date))) * 24 * 60 * 60) as session_duration_sec
       
       -- Quantity of events
     , SUM(1) as event_qty
     , SUM(CASE WHEN event_type = "page_view" THEN 1 ELSE 0 END) as page_view_qty
     , SUM(CASE WHEN event_type = "add_to_cart" THEN 1 ELSE 0 END) as add_to_cart_qty
     , SUM(CASE WHEN event_type = "order" THEN 1 ELSE 0 END) as order_qty

        -- Event flags
     , MAX(CASE WHEN event_type = "page_view" THEN 1 ELSE 0 END) as has_event_page_view
     , MAX(CASE WHEN event_type = "add_to_cart" THEN 1 ELSE 0 END) as has_event_add_to_cart
     , MAX(CASE WHEN event_type = "order" THEN 1 ELSE 0 END) as has_event_order

        -- Page type flags
      , MAX(CASE WHEN event_type = "page_view" AND page_type = "product_page" THEN 1 ELSE 0 END) as has_page_pdp
      , MAX(CASE WHEN event_type = "page_view" AND page_type = "listing_page" THEN 1 ELSE 0 END) as has_page_plp
      , MAX(CASE WHEN event_type = "page_view" AND page_type = "search_listing_page" THEN 1 ELSE 0 END) as has_page_search_plp
      , MAX(CASE WHEN event_type = "page_view" AND page_type = "order_page" THEN 1 ELSE 0 END) as has_page_order

        -- Page type quantity
      , SUM(CASE WHEN event_type = "page_view" AND page_type = "product_page" THEN 1 ELSE 0 END) as page_pdp_qty
      , SUM(CASE WHEN event_type = "page_view" AND page_type = "listing_page" THEN 1 ELSE 0 END) as page_plp_qty
      , SUM(CASE WHEN event_type = "page_view" AND page_type = "search_listing_page" THEN 1 ELSE 0 END) as page_search_plp_qty
      , SUM(CASE WHEN event_type = "page_view" AND page_type = "order_page" THEN 1 ELSE 0 END) as page_order_qty

        -- add_to_cart (bag/a2b) flags
      , MAX(CASE WHEN event_type = "add_to_cart" AND page_type = "product_page" THEN 1 ELSE 0 END) as has_a2b_pdp
      , MAX(CASE WHEN event_type = "add_to_cart" AND page_type = "listing_page" THEN 1 ELSE 0 END) as has_a2b_plp
      , MAX(CASE WHEN event_type = "add_to_cart" AND page_type = "search_listing_page" THEN 1 ELSE 0 END) as has_a2b_search_plp
 
        -- add_to_cart (bag/a2b) quantity
      , SUM(CASE WHEN event_type = "add_to_cart" AND page_type = "product_page" THEN 1 ELSE 0 END) as a2b_pdp_qty
      , SUM(CASE WHEN event_type = "add_to_cart" AND page_type = "listing_page" THEN 1 ELSE 0 END) as a2b_plp_qty
      , SUM(CASE WHEN event_type = "add_to_cart" AND page_type = "search_listing_page" THEN 1 ELSE 0 END) as a2b_search_plp_qty

FROM data_set_da_test

GROUP BY 1, 2)
--

, windowed_session_info as ( 
SELECT 
        -- Here'll just add # session_number (based on the user lifecycle) and the timestamp of next/previous session
        general_session_info.*
      , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_start) AS session_number
      , LEAD(session_start) OVER (PARTITION BY user_id ORDER BY session_start) AS next_session_start
      , LAG(session_start) OVER (PARTITION BY user_id ORDER BY session_start) AS last_session_start
 
FROM general_session_info)
--

, exit_land_support as (
SELECT 
       -- Finally we'll grab the landing and exit pages
       session as session_id
     , event_type
     , page_type
     , ROW_NUMBER() OVER (PARTITION BY session ORDER BY event_date) as rown_asc
     , ROW_NUMBER() OVER (PARTITION BY session ORDER BY event_date DESC) as rown_desc

FROM data_set_da_test)
--

SELECT  main.*
      , sup_land.page_type as landing_page
      , sup_exit.page_type as exit_page
      , ROUND((JULIANDAY(next_session_start) - JULIANDAY(session_start))) as days_from_last_session
      , ROUND((JULIANDAY(session_start) - JULIANDAY(last_session_start))) as days_to_next_session

FROM windowed_session_info main
LEFT JOIN exit_land_support sup_land ON main.session_id = sup_land.session_id AND sup_land.rown_asc = 1
LEFT JOIN exit_land_support sup_exit ON main.session_id = sup_exit.session_id AND sup_exit.rown_desc = 1
;