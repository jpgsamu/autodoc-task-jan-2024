select page_type
,sum(session_atc_qty) as atc_sessions
        , sum(atc_event_qty) as atc_events
from tb_pages
group by 1