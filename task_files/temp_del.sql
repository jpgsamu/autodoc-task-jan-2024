with t1 as (
select session 
, max(case when event_type = "add_to_cart" then 1 else 0 end) as has_a2b
, max(Case when event_type = "order" then 1 else 0 end) as has_order
from data_set_da_test
group by 1)
select has_a2b, has_order, sum(1) as qty

from (t1)

group by 1,2