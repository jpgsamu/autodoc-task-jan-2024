-- How many records with have daily & what is our timeframe?

SELECT DATE(event_date) as event_date
     , SUM(1) as qty 
 
FROM data_set_da_test

GROUP BY 1
ORDER BY 1

-- NOTE: We have 2 weeks of data and avg. 45k events daily (637k total)
------------------------------------------------------------------------------------------
-- What are the event types we have?

SELECT event_type
     , SUM(1) as qty 
 
FROM data_set_da_test

GROUP BY 1
ORDER BY 2 DESC

-- NOTE: We have 3 event types: "page_view" (~612k), "add_to_cart" (~16k) and "order" (~9k)
------------------------------------------------------------------------------------------
-- How does "product" work? 

SELECT event_type
     , SUM(1) as records
     , SUM(CASE WHEN product = 0 THEN 1 ELSE 0 END) as product_0_records
     , SUM(CASE WHEN product <> 0 THEN 1 ELSE 0 END) as product_not_0_records
     , SUM(CASE WHEN product IS NULL THEN 1 ELSE 0 END) as product_null_records
 
FROM data_set_da_test

GROUP BY 1
ORDER BY 2 DESC

-- NOTE: "page_view" and "order" always have '0' as 'product' but 'add_to_cart' receives a product ID
------------------------------------------------------------------------------------------
-- What are the page types? What events are triggered in each?

SELECT page_type
     , event_type
     , SUM(1) as records
 
FROM data_set_da_test

GROUP BY 1, 2
ORDER BY 1, 3 DESC

-- NOTE:
-- (1) "order_page": only triggers the "order" event
-- (2) "listing_page" (PLP): triggers "page_view" and "add_to_cart"
-- (3) "search_listing_page" (search PLP): triggers "page_view" and "add_to_cart"
-- (4) "product_page" (search PLP): triggers "page_view" and "add_to_cart"
-- !!!!!! "add_to_cart" is greater on search PLP vs. PLP - maybe ranking algorithm and/or visual merchandise can be optimized? need to check CTRs
------------------------------------------------------------------------------------------
