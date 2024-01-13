/* 
PROMPT 1: Write an SQL query that will return a number of clients by day that:

(1) only viewed products in their first session;
(2) added only one product to the basket; 
(3) placed an order within two days time after the first session.

-> Clarifications:

(1) I'll approach this in two ways: "lifecycle" and "daily"
 * lifecycle -> user's first session ever, in the entire lifecycle (dataset)
 * lifecycle -> user's first session daily, in a given day

 Also, I'll consider "view product" as "page_view" event, since it's triggered on listing and product pages, I'm assuming both count as "viewing" a product.

(2) I'll approach this in two ways: "only 1 ATC event/daily" and "only 1 product_id ATC/daily"
* only 1 ATC event/daily -> Only 1 event of type "add_to_cart" on a given day.
* only 1 product_id ATC/daily -> Only 1 product ID on "add_to_cart" events daily, NOTE: the product can be added multiple times daily, as long as it's the only product_id.

(3) I'll approach this in two ways: "lifecycle" and "daily"
 * lifecycle -> user's first session ever, in the entire lifecycle (dataset)
 * lifecycle -> user's first session daily, in a given day
 
-> Outcome description:

* users_only_view_products_on_1st_session_lifecycle_qty - Qty of users that only view produts on their first session ever in the entire dataset (lifecycle), rooted on the first session date.
* users_only_view_products_on_1st_session_daily_qty - Qty of users that only view produts on their first session ever of a given day, rooted on that given day.

* users_only_1_atc_event_on_day_qty - Qty of users that only have 1 ATC event daily, rooted on the event date.
* users_only_1_product_id_atc_on_day_qty - Qty of users that only have 1 "product_id" added to cart daily (1 or more times), rooted on the event date.

* user_has_order_within_48h_after_1st_session_lifecycle_qty - Qty of users that place an orden within 48h after their first ever session in the entire dataset (lifecycle), rooted on the first session date.
* user_has_order_within_48h_after_1st_session_daily_qty - Qty of users that place an order within 48h of after their first sesssion on a given day, rooted on that given day.

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

      , MIN(CASE WHEN event_type = "order" THEN event_date ELSE NULL END) as session_min_order_timestamp

FROM data_set_da_test 

GROUP BY 1, 2)
--

, tb_sessions_rown_life as ( -- Now we'll create a "session number" index for each session ID

SELECT  tb_sessions.*
      , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_start_timestamp) as session_number_life

FROM tb_sessions)
--

, tb_days_until_order_lifecycle as ( -- This will capture the timestamp difference between a session's first order event and its user's first ever session start
SELECT tb_order.session_id
     , tb_order.user_id

     , tb_order.session_min_order_timestamp as order_timestamp
     , tb_first.session_start_timestamp as first_sesion_timetamp
     , tb_first.session_start_date as first_session_date

     , JULIANDAY(tb_order.session_min_order_timestamp) - JULIANDAY(tb_first.session_start_timestamp) as delta_time_days

FROM tb_sessions_rown_life tb_order
INNER JOIN tb_sessions_rown_life tb_first ON tb_order.user_id = tb_first.user_id
                                          AND tb_first.session_number_life = 1
                                     
WHERE tb_order.has_event_order = 1)
--

, tb_question1_lifecycle as ( -- users that only view products in their first ever session within the entire dataset (lifecycle), rooted on the 1st session date
SELECT  session_start_date as date_ref
      , SUM(1) as users_only_view_products_on_1st_session_lifecycle_qty

FROM tb_sessions_rown_life

WHERE session_number_life = 1
AND has_event_page_view = 1
AND has_event_atc = 0
AND has_event_order = 0

GROUP BY 1)
--

, tb_question3_lifecycle as (  -- users that placed an order within 2 days after their first ever session within the entire dataset (lifecycle), rooted on the 1st session date
SELECT  first_session_date as date_ref
      , COUNT(DISTINCT CASE WHEN delta_time_days <= 2 THEN user_id ELSE NULL END) as user_has_order_within_48h_after_1st_session_lifecycle_qty

FROM tb_days_until_order_lifecycle

GROUP BY 1
ORDER BY 1)
--

, tb_sessions_rown_daily as ( -- Now we'll create a "session number" index for each user daily sessions

SELECT  tb_sessions.*
      , ROW_NUMBER() OVER (PARTITION BY user_id, session_start_date ORDER BY session_start_timestamp) as session_number_daily

FROM tb_sessions)
--

, tb_days_until_order_daily as ( -- This will capture the timestamp difference between a session's first order event and its user's list of "first daily session" timestamps
SELECT tb_order.session_id
     , tb_order.user_id

     , tb_order.session_min_order_timestamp as order_timestamp
     , tb_first.session_start_timestamp as first_sesion_timetamp
     , tb_first.session_start_date as first_session_date

     , JULIANDAY(tb_order.session_min_order_timestamp) - JULIANDAY(tb_first.session_start_timestamp) as delta_time_days

FROM tb_sessions_rown_daily tb_order
INNER JOIN tb_sessions_rown_daily tb_first ON tb_order.user_id = tb_first.user_id
                                           AND tb_first.session_number_daily = 1
                                     
WHERE tb_order.has_event_order = 1)
--

, tb_question1_daily as ( -- users that only view products in their first daily session on a given day, rooted on the session start date daily
SELECT  session_start_date as date_ref
      , SUM(1) as users_only_view_products_on_1st_session_daily_qty

FROM tb_sessions_rown_daily

WHERE session_number_daily = 1
AND has_event_page_view = 1
AND has_event_atc = 0
AND has_event_order = 0

GROUP BY 1)
--

, tb_question3_daily as (  -- users that placed an order within 2 days after their first session within a given day, rooted on the session start date daily
SELECT  first_session_date as date_ref
      , COUNT(DISTINCT CASE WHEN delta_time_days <= 2 THEN user_id ELSE NULL END) as user_has_order_within_48h_after_1st_session_daily_qty

FROM tb_days_until_order_daily

GROUP BY 1
ORDER BY 1)
--
, tb_users_daily as ( -- This subquery will aggregate user metrics per user_id daily
SELECT  user as user_id
      , DATE(event_date) as event_date
      , SUM(CASE WHEN event_type = "add_to_cart" THEN 1 ELSE 0 END) as ATC_events_daily_qty
      , COUNT(DISTINCT CASE WHEN event_type = "add_to_cart" THEN product ELSE NULL END) as dist_products_ATC_daily_qty     

FROM data_set_da_test 

GROUP BY 1, 2)
--

, tb_question2 as ( -- Now we can get users that only have only 1 daily ATC event (a) and have 1 or + daily ATC but only 1 product_id (b)
SELECT  event_date as date_ref
      , SUM(CASE WHEN ATC_events_daily_qty = 1 THEN 1 ELSE 0 END) as users_only_1_atc_event_on_day_qty
      , SUM(CASE WHEN dist_products_ATC_daily_qty = 1 THEN 1 ELSE 0 END) as users_only_1_product_id_atc_on_day_qty

FROM tb_users_daily

GROUP BY 1)
--

SELECT  tb_question1_lifecycle.date_ref

        -- Question 1
      , users_only_view_products_on_1st_session_lifecycle_qty
      , users_only_view_products_on_1st_session_daily_qty

      -- Question 2
      , users_only_1_atc_event_on_day_qty 
      , users_only_1_product_id_atc_on_day_qty

         -- Question 3
      , user_has_order_within_48h_after_1st_session_lifecycle_qty
      , user_has_order_within_48h_after_1st_session_daily_qty
      


FROM tb_question1_lifecycle
LEFT JOIN tb_question1_daily ON tb_question1_lifecycle.date_ref = tb_question1_daily.date_ref

LEFT JOIN tb_question2 ON tb_question1_lifecycle.date_ref = tb_question2.date_ref

LEFT JOIN tb_question3_lifecycle ON tb_question1_lifecycle.date_ref = tb_question3_lifecycle.date_ref
LEFT JOIN tb_question3_daily ON tb_question1_lifecycle.date_ref = tb_question3_daily.date_ref

ORDER BY 1