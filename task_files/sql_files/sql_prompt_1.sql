/* 
PROMPT 1: Write an SQL query that will return a number of clients by day that:

(1) only viewed products in their first session;
(2) added only one product to the basket; 
(3) placed an order within two days time after the first session.

-> Assumptions:
# (1a) "first session" will be considered an user's first session within the entire dataset (lifecycle) and not the first session of a given day.
# (1b) "viewed products" will be considered "page_view" events since it happens on product and listing pages - both type of pages generate product impressions.
# (2) Will be based on  "one add to cart event per day" - meaning I will not account for users that pontentially added "only one product ID" but multiple times.
# (3) "within two days" will be based on "timestamp" i.e. inside a 48h threshold



*/

with tb_sessions as ( -- This subquery will aggregate session metrics per session ID

SELECT  user as user_id
      , session as session_id

      , DATE(MIN(event_date)) as session_start_date
      , MIN(event_date) as session_start_timestamp

      , MAX(CASE WHEN event_type = "page_view" THEN 1 ELSE 0 END) as has_event_page_view
      , MAX(CASE WHEN event_type = "add_to_cart" THEN 1 ELSE 0 END) as has_event_atc
      , MAX(CASE WHEN event_type = "order" THEN 1 ELSE 0 END) as has_event_order

      , SUM(CASE WHEN event_type = "page_view" THEN 1 ELSE 0 END) as qty_event_page_view
      , SUM(CASE WHEN event_type = "add_to_cart" THEN 1 ELSE 0 END) as qty_event_atc
      , SUM(CASE WHEN event_type = "order" THEN 1 ELSE 0 END) as qty_event_order

      , COUNT(DISTINCT CASE WHEN event_type = "add_to_cart" THEN product END) as qty_dist_products_atc
      , MIN(CASE WHEN event_type = "order" THEN event_date ELSE NULL END) as session_min_order_timestamp

FROM data_set_da_test 

GROUP BY 1, 2)
--

, tb_sessions_rown as ( -- Now we'll create a "session number" index for each session ID

SELECT  tb_sessions.*
      , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_start_timestamp) as session_number

FROM tb_sessions)
--

, tb_days_until_order as ( -- This will capture the timestamp difference between a session's first order an event and its user's first ever session start
SELECT tb_order.session_id
     , tb_order.user_id

     , tb_order.session_min_order_timestamp as order_timestamp
     , tb_first.session_start_timestamp as first_sesion_timetamp
     , tb_first.session_start_DATE as first_sesion_date

     , JULIANDAY(tb_order.session_min_order_timestamp) - JULIANDAY(tb_first.session_start_timestamp) as delta_time_days

FROM tb_sessions_rown tb_order
INNER JOIN tb_sessions_rown tb_first ON tb_order.user_id = tb_first.user_id
                                     AND tb_first.session_number = 1
                                     
WHERE tb_order.has_event_order = 1)
--

, tb_question1 as (
SELECT

FROM tb_sessions

WHERE 

)


