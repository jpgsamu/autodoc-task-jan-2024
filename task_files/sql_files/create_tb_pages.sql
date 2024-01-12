CREATE TABLE tb_pages AS

with tb_main as (
SELECT session_date
     , page_type
     , COUNT(DISTINCT session) as session_qty
     , COUNT(DISTINCT CASE WHEN aux.has_event_order = 1 THEN session ELSE NULL END) as session_converted_qty
     , SUM(CASE WHEN event_type = "page_view" THEN 1 ELSE 0 END) as pageview_event_qty
     , SUM(CASE WHEN event_type = "add_to_cart" THEN 1 ELSE 0 END) as atc_event_qty
     
FROM data_set_da_test main
LEFT JOIN tb_sessions aux ON main.session = aux.session_id

GROUP BY 1, 2)
--
, tb_LP as (
SELECT session_date
     , landing_page as page_type
     , SUM(1) as landing_session_qty
     , SUM(has_event_order) as landing_session_converted_qty
     , SUM(session_duration_sec) as landing_session_duration_acc

FROM tb_sessions

GROUP BY 1,2)
--
, tb_EP as (
SELECT session_date
     , exit_page as page_type
     , SUM(1) as exit_session_qty
     , SUM(CASE WHEN event_qty = 1 THEN 1 ELSE 0 END) as bounce_session_qty

FROM tb_sessions

GROUP BY 1,2)
--
, tb_combined as (
SELECT  tb_main.session_date
      , tb_main.page_type
      , tb_main.session_qty
      , tb_main.session_converted_qty
      , tb_main.pageview_event_qty
      , tb_main.atc_event_qty

      , COALESCE(tb_LP.landing_session_qty, 0) as landing_session_qty
      , COALESCE(tb_LP.landing_session_converted_qty, 0) as landing_session_converted_qty
      , COALESCE(tb_LP.landing_session_duration_acc, 0) as landing_session_duration_acc

      , COALESCE(tb_EP.exit_session_qty, 0) as exit_session_qty
      , COALESCE(tb_EP.bounce_session_qty, 0) as bounce_session_qty

FROM tb_main
LEFT JOIN tb_LP ON tb_main.session_date = tb_LP.session_date AND tb_main.page_type = tb_LP.page_type
LEFT JOIN tb_EP ON tb_main.session_date = tb_EP.session_date AND tb_main.page_type = tb_EP.page_type

ORDER BY 1, 2)
--
, tb_agg as (
SELECT session_date
     , SUM(1) as session_qty_total
     , SUM(page_view_qty) as pageview_event_qty_total
     , SUM(add_to_cart_qty) as atc_event_qty_total

FROM tb_sessions

GROUP BY 1)
--

SELECT tb_combined.*
     , tb_agg.session_qty_total
     , tb_agg.pageview_event_qty_total
     , tb_agg.atc_event_qty_total

FROM tb_combined
LEFT JOIN tb_agg ON tb_combined.session_date = tb_agg.session_date
;