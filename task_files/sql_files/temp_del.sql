select sum(session_atc_qty) as atc_sessions
        , sum(atc_event_qty) as atc_events
from tb_pages
where page_type = "listing_page"