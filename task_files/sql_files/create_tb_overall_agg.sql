CREATE TABLE tb_daily_overall_agg as

SELECT DATE(main.event_date) as event_date

     , COUNT(DISTINCT main.session) as session_qty
     , COUNT(DISTINCT CASE WHEN tbs.event_qty = 1 THEN main.session ELSE NULL END) as bounce_sessions_qty
     
     , COUNT(DISTINCT main.user) as users_qty
     , COUNT(DISTINCT CASE WHEN tbs.session_number = 1 THEN main.user ELSE NULL END) as new_users_qty

     , COUNT(DISTINCT CASE WHEN main.event_type = "add_to_cart" THEN main.session ELSE NULL END) as atc_session_qty
     , COUNT(DISTINCT CASE WHEN main.event_type = "order" THEN main.session ELSE NULL END) as order_session_qty
     , SUM(CASE WHEN main.event_type = "page_view" THEN 1 ELSE 0 END) as page_view_qty    

 FROM data_set_da_test main
 LEFT JOIN tb_sessions tbs ON main.session = tbs.session_id

 GROUP BY 1
 ORDER BY 1
 ;