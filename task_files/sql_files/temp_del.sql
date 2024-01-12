with tb_main as (
SELECT DATE(event_date) as date_ref
     , page_type
     , COUNT(DISTINCT session) as session_qty
     , COUNT(DISTINCT CASE WHEN aux.has_event_order = 1 THEN session ELSE NULL END) as session_converted_qty
     , SUM(CASE WHEN event_type = "page_view" THEN 1 ELSE 0 END) as pageview_event_qty
     , SUM(CASE WHEN event_type = "add_to_cart" THEN 1 ELSE 0 END) as atc_event_qty
     
FROM data_set_da_test main
LEFT JOIN tb_sessions aux ON main.session = aux.session_id

GROUP BY 1, 2)
--
select sum(session_qty) from tb_main
