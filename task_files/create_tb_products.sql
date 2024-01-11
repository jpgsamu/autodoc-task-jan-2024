CREATE TABLE tb_products as 

with product_sessions as (
SELECT     
          -- Number of add to carts per session and product
          product as product_id
        , session as session_id
        , SUM(1) as a2b_qty
        , SUM(CASE WHEN page_type = 'product_page' THEN 1 ELSE 0 END) as pdp_a2b_qty
        , SUM(CASE WHEN page_type = 'listing_page' THEN 1 ELSE 0 END) as plp_a2b_qty
        , SUM(CASE WHEN page_type = 'search_listing_page' THEN 1 ELSE 0 END) as search_plp_a2b_qty
        , MIN(event_date) as first_a2b_timestamp
        
FROM data_set_da_test

WHERE event_type = "add_to_cart"

GROUP BY 1,2)
--

, previous_pages as (
SELECT
         -- Captures the pageviews seen before an "add_to_cart" event
          ps.product_id
        , ps.session_id
        , ps.a2b_qty
        , ps.pdp_a2b_qty
        , ps.plp_a2b_qty
        , ps.search_plp_a2b_qty
        , ps.first_a2b_timestamp
        , SUM(1) as page_qty_before_a2b
        , SUM(CASE WHEN page_type = "product_page" THEN 1 ELSE 0 END) as pdp_qty_before_a2b
        , SUM(CASE WHEN page_type = "listing_page" THEN 1 ELSE 0 END) as plp_qty_before_a2b
        , SUM(CASE WHEN page_type = "search_listing_page" THEN 1 ELSE 0 END) as search_plp_qty_before_a2b

FROM product_sessions ps
LEFT JOIN data_set_da_test main ON main.session = ps.session_id
                                AND main.event_date <= ps.first_a2b_timestamp
                                AND main.event_type = "page_view"

GROUP BY 1, 2, 3, 4, 5, 6, 7
)
--

, final_sessions_products as (
SELECT pp.*
     , tb_sessions.has_event_order as session_has_order

FROM previous_pages pp 
JOIN tb_sessions ON pp.session_id = tb_sessions.session_id)
--

SELECT    
          -- Final Product ID dataset
          product_id

        , SUM(1) as sessions_a2b_qty
        , SUM(session_has_order) as sessions_with_order

        , SUM(a2b_qty) as events_a2b_qty
        , SUM(pdp_a2b_qty) as events_pdp_a2b_qty
        , SUM(plp_a2b_qty) as events_plp_a2b_qty
        , SUM(search_plp_a2b_qty) as events_search_plp_a2b_qty

        , SUM(page_qty_before_a2b) as pages_before_a2b_qty
        , SUM(pdp_qty_before_a2b) as pdp_before_a2b_qty
        , SUM(plp_qty_before_a2b) as plp_before_a2b_qty
        , SUM(search_plp_qty_before_a2b) as search_plp_before_a2b_qty
        
FROM final_sessions_products

GROUP BY 1
ORDER BY 2 DESC
;
