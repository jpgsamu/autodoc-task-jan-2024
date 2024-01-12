CREATE TABLE tb_funnel_1 as 
SELECT session_date
     , landing_page
     , exit_page
     , SUM(1) as sessions_total
     , SUM(CASE WHEN has_page_pdp = 1 OR 
                     has_page_plp = 1 OR 
                     has_page_search_plp = 1 THEN 1 ELSE 0 END) as sessions_with_impression

     , SUM(has_page_pdp) as sessions_with_pdp

     , SUM(CASE WHEN has_page_plp = 1 OR  has_page_search_plp = 1 THEN 1 ELSE 0 END) as sessions_with_plp

     , SUM(CASE WHEN (has_page_plp = 1 OR has_page_search_plp = 1) AND 
                      has_page_pdp = 1 THEN 1 ELSE 0 END) as sessions_with_plp_and_pdp

     , SUM(CASE WHEN has_page_pdp = 1 AND has_event_add_to_cart = 1 THEN 1 ELSE 0 END) as sessions_with_pdp_and_a2b
                     
     , SUM(has_event_add_to_cart) as sessions_with_a2b
     , SUM(has_event_order) as sessions_with_order

     , SUM(CASE WHEN (has_page_pdp = 1 OR 
                      has_page_plp = 1 OR 
                      has_page_search_plp = 1) AND
                      has_event_add_to_cart = 1 THEN 1 ELSE 0 END) as sessions_with_impression_and_a2b

     , SUM(CASE WHEN has_event_add_to_cart = 1 AND
                     has_event_order = 1 THEN 1 ELSE 0 END) as sessions_with_a2b_and_order



FROM tb_sessions

GROUP BY 1, 2, 3
;