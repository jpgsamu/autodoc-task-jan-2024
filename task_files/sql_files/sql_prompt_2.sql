/* 

PROMPT 2: Write a query that will return any abnormal (to our view) user behavior. Describe why the behavior is unusual

Here's my list of tests:

-----------------------------------
# Potential Tracking Issues
-----------------------------------
* tracking_issue_1 (ti1) -> Sessions that don't have "page_view" event: This is likely a tracking issue since if the user performs any other action he/she must have seen at least 1 page of the website first.

* tracking_issue_2 (ti2) -> Sessions in which "page_view" is not the first event: Also a potential tracking issue in the order of events being triggered, assumption is the same of test #1.

* tracking_issue_3 (ti3) -> Sessions that have "product_page" events but no "page_view" event: Another potential hint of missing "page_view" events.
* tracking_issue_4 (ti4) -> Sessions that have "listing_page" events but no "page_view" event: Another potential hint of missing "page_view" events.
* tracking_issue_5 (ti5) -> Sessions that have "search_listing_page" events but no "page_view" event: Another potential hint of missing "page_view" events.
* tracking_issue_6 (ti6) -> Sessions that have "order_page" events but no "page_view" event: Another potential hint of missing "page_view" events.

* tracking_issue_7 (ti7) -> Users that have no "add_to_cart" events in between orders. Potential traking issues on "add_to_cart" events.ABORT

-----------------------------------
# Potential Bot and Fraud Behaviour
-----------------------------------
* suspicious_users_1 (su1) -> Users that have "+10" sessions on a given day. Arbitrary threshold that may be refined on future studies with the distribution. Might be price_checker bots.
* suspicious_users_2 (su2) -> Users that have "+100" page_view events on a given day. Arbitrary threshold that may be refined on future studies with the distribution. Might be price_checker bots.
* suspicious_users_3 (su3) -> Users that have "+10" order events on a given day. Arbitrary threshold that may be refined on future studies with the distribution. Might be fraud.

-----------------------------------
# Potential Website Malfunction
-----------------------------------
* bounce_rate -> Sessions that only have 1 event. If the rate is high, users might be facing website malfunctions and exiting rapidly.
* avg_session_duration -> Sessions average duration, supports bounce_rate in identifying potential malfunctions.

*/

with tb_sessions_agg as ( -- This subquery will aggregate session metrics per session ID
SELECT  user as user_id
      , session as session_id

      , DATE(MIN(event_date)) as session_start_date

      , MIN(event_date) as session_start_timestamp
      , MAX(event_date) as session_final_timestamp
      , ROUND((JULIANDAY(MAX(event_date)) - JULIANDAY(MIN(event_date))) * 24 * 60 * 60) as session_duration_secs

      , MAX(CASE WHEN event_type = "page_view" THEN 1 ELSE 0 END) as has_event_page_view
      , MAX(CASE WHEN event_type = "add_to_cart" THEN 1 ELSE 0 END) as has_event_atc
      , MAX(CASE WHEN event_type = "order" THEN 1 ELSE 0 END) as has_event_order

      , SUM(CASE WHEN event_type = "page_view" THEN 1 ELSE 0 END) as qty_event_page_view
      , SUM(CASE WHEN event_type = "add_to_cart" THEN 1 ELSE 0 END) as qty_event_atc
      , SUM(CASE WHEN event_type = "order" THEN 1 ELSE 0 END) as qty_event_order
      , SUM(1) as qty_event_any

      , MAX(CASE WHEN page_type = "product_page" AND event_type = "page_view" THEN 1 ELSE 0 END) as has_pdp_view
      , MAX(CASE WHEN page_type = "product_page" AND event_type <> "page_view" THEN 1 ELSE 0 END) as has_pdp_action

      , MAX(CASE WHEN page_type = "listing_page" AND event_type = "page_view" THEN 1 ELSE 0 END) as has_plp_view
      , MAX(CASE WHEN page_type = "listing_page" AND event_type <> "page_view" THEN 1 ELSE 0 END) as has_plp_action

      , MAX(CASE WHEN page_type = "search_listing_page" AND event_type = "page_view" THEN 1 ELSE 0 END) as has_search_view
      , MAX(CASE WHEN page_type = "search_listing_page" AND event_type <> "page_view" THEN 1 ELSE 0 END) as has_search_action

FROM data_set_da_test 

GROUP BY 1, 2)
--

, tb_users_daily as ( -- This subquery will aggregate user metrics daily
SELECT  user as user_id
      , DATE(event_date) as event_date

      , COUNT(DISTINCT session) as session_qty
      , SUM(CASE WHEN event_type = "page_view" THEN 1 ELSE 0 END) as pageview_qty
      , SUM(CASE WHEN event_type = "page_view" THEN 1 ELSE 0 END) as order_qty

FROM data_set_da_test 

GROUP BY 1, 2)
--

, tb_numbered_session_events as ( -- Here will add a window function within sessions
SELECT  session as session_id
      , event_type
      , ROW_NUMBER () OVER (PARTITION BY session ORDER BY event_date) as rown

FROM data_set_da_test )
--

, tb_RN_atc_and_orders as ( -- Here will add a window function within users orders and ATC events
SELECT  user as user_id
      , event_type || event_date as event_id
      , event_date
      , event_type
      , ROW_NUMBER () OVER (PARTITION BY user ORDER BY event_date) as rown_atc_and_order

FROM data_set_da_test 

WHERE event_type IN ("add_to_cart","order"))
--

, tb_RN_atc as ( -- Here will add a window function within users ATC events only
SELECT  user as user_id
      , event_type || event_date as event_id
      , ROW_NUMBER () OVER (PARTITION BY user ORDER BY event_date) as rown_atc

FROM data_set_da_test 

WHERE event_type = "add_to_cart")
--

, tb_RN_orders as ( -- Here will add a window function within users order events only
SELECT  user as user_id
      , event_type || event_date as event_id
      , ROW_NUMBER () OVER (PARTITION BY user ORDER BY event_date) as rown_order

FROM data_set_da_test 

WHERE event_type = "order")
--

, tb_atc_and_orders as ( -- Combines the 3 row numbers per event id
SELECT   tb_RN_atc_and_orders.user_id
       , tb_RN_atc_and_orders.event_id
       , tb_RN_atc_and_orders.event_date

       , rown_atc_and_order
       , rown_atc
       , rown_order

FROM tb_RN_atc_and_orders
LEFT JOIN tb_RN_atc ON tb_RN_atc_and_orders.user_id = tb_RN_atc.user_id
                    AND tb_RN_atc_and_orders.event_id = tb_RN_atc.event_id
LEFT JOIN tb_RN_orders ON tb_RN_atc_and_orders.user_id = tb_RN_orders.user_id
                       AND tb_RN_atc_and_orders.event_id = tb_RN_orders.event_id)
--

, tb_atc_and_orders_aux as ( -- Aux table that iterates between row numbers
SELECT  main.event_id
      , MAX(aux1.rown_atc_and_order) as last_atc_rown_atc_and_order
      , MAX(aux2.rown_atc_and_order) as last_order_rown_atc_and_order

FROM tb_atc_and_orders main
LEFT JOIN tb_atc_and_orders aux1 ON main.user_id = aux1.user_id
                                 AND aux1.event_id LIKE "add_to_cart%"
                                 AND main.rown_atc_and_order > aux1.rown_atc_and_order
LEFT JOIN tb_atc_and_orders aux2 ON main.user_id = aux2.user_id
                                 AND aux2.event_id LIKE "order%"
                                 AND main.rown_atc_and_order > aux2.rown_atc_and_order  

WHERE main.event_id LIKE "order%"
GROUP BY 1)
--

, tb_atc_and_orders_final as ( -- Finally combines the two last tables on event_id
SELECT  main.*

      , aux.last_atc_rown_atc_and_order
      , aux.last_order_rown_atc_and_order

      , CASE WHEN main.event_id LIKE "add_to_cart%" THEN "ATC"
             WHEN main.event_id LIKE "order%" AND last_atc_rown_atc_and_order IS NULL THEN "Unusual Order" -- never had ATC before order
             WHEN main.event_id LIKE "order%" AND last_atc_rown_atc_and_order < last_order_rown_atc_and_order THEN "Unusual Order" -- no ATC since last order
             ELSE "Regular Order" END as atc_and_order_type

FROM tb_atc_and_orders main
LEFT JOIN tb_atc_and_orders_aux aux ON main.event_id = aux.event_id)
--
