
----------------------------------------------------------------------------------------------------------------
-- Creating the Silver layer Star Schema

//Device Family Lookup
//SQL code example of how it is implemented in snowflake
SELECT DISTINCT HASH("device_family") as device_family_id, 
"device_family" as device_family
FROM B_AMPLITUDE_EVENTS;

//Device_Lookup
SELECT DISTINCT HASH("device_type") as device_type_id, 
NVL( "device_type","device_family")as device_name,
HASH("device_family") as device_family_id
FROM B_AMPLITUDE_EVENTS;

//OS_lookup
SELECT DISTINCT HASH("os_name") as os_id, 
"os_name" as os_name
FROM B_AMPLITUDE_EVENTS;

//Device
SELECT DISTINCT HASH("os_name", "device_type", "device_family", "os_version") as device_id,
HASH("os_name") os_id,
HASH("device_type") device_type_id,
"os_version" as os_version
FROM B_AMPLITUDE_EVENTS;

//Country
CREATE OR REPLACE TABLE S_AMPLITUDE_COUNTRY as
select distinct
"country" as country_name,
hash("country") as country_id
from B_amplitude_events;

---- Region ------
CREATE OR REPLACE TABLE S_AMPLITUDE_REGION AS
select distinct
"region" as region_name,
hash("region") as region_id
from B_amplitude_events;

------ City -------
CREATE OR REPLACE TABLE S_AMPLITUDE_CITY AS
select distinct
"city" as city_name,
hash("city") as city_id
from B_amplitude_events;

---- Location ------
CREATE OR REPLACE TABLE S_AMPLITUDE_LOCATION AS
select distinct
hash("ip_address") as location_id,
"ip_address" as ip_address,
hash("city") as city_id,
hash("country") as country_id,
hash("region") as region_id
from B_amplitude_events;


------ Sessions ------
CREATE OR REPLACE TABLE S_AMPLITUDE_SESSION AS
SELECT DISTINCT
    "session_id" AS session_id,
    "user_id" AS user_id,
    HASH("os_name", "device_type", "device_family", "os_version") device_id,
    HASH("ip_address") AS location_id
FROM b_amplitude_events;

------ Events List -----
CREATE OR REPLACE TABLE S_AMPLITUDE_EVENTS_LIST AS
select distinct
    el."id" as events_list_id,
    el."name" as event_name
from b_amplitude_events_lists as el
;


------ Events -----
CREATE OR REPLACE TABLE S_AMPLITUDE_EVENTS AS
SELECT DISTINCT
     eve."uuid"               AS event_id
    ,eve."session_id"         AS session_id
    ,eve."event_id"           AS session_event_order
    ,evel."id"                AS events_list_id
    ,eve."event_time"         AS event_time
    ,HASH(eve."user_properties") AS user_properties_id
    ,HASH(eve."event_properties") AS event_properties_id
FROM b_amplitude_events eve
LEFT JOIN b_amplitude_events_lists evel
    ON eve."event_type" = evel."name";

------ Events Properties ------
CREATE OR REPLACE TABLE S_AMPLITUDE_EVENT_PROPERTIES AS
with parse_evp_json_cte as (
    select distinct
        hash(e."event_properties") as event_properties_id,
        (parse_json(e."event_properties")) as json
    from b_amplitude_events as e
)

select distinct
    event_properties_id,
    json:"[Amplitude] Page URL"::string as page_url,
    json:"referrer"::string as referrer,-- like google
    json:"[Amplitude] Page Counter"::int as page_counter,
    json:"[Amplitude] Page Domain"::string as page_domain, --like til
    json:"[Amplitude] Page Path"::string as page_path, --like /how-we-help
    json:"[Amplitude] Page Title"::string as page_title, --the nice one
    json:"[Amplitude] Page Location"::string as page_location, --page_domain with page_path
    json:"referring_domain"::string as referring_domain, -- like google
    json:"[Amplitude] Element Text"::string as element_text, --like accept
    json:"video_url"::string as video_url, --like embed link
    -- json:"" as 
from parse_evp_json_cte as e
;

---- User Properties ------
CREATE OR REPLACE TABLE S_AMPLITUDE_USER_PROPERTIES AS
with json as (Select
    hash("user_properties") as up_id,
    parse_json("user_properties") as parsed_json
from b_amplitude_events
)

select distinct
    up_id,
    parsed_json:initial_utm_medium::STRING AS initial_utm_medium,
    parsed_json:initial_referring_domain::STRING AS initial_referring_domain,
    parsed_json:initial_utm_campaign::STRING AS initial_utm_campaign,
    parsed_json:referrer::STRING AS referrer,
    parsed_json:initial_utm_source::STRING AS initial_utm_source,
    parsed_json:initial_referrer::STRING AS initial_referrer,
    parsed_json:referring_domain::STRING AS referring_domain
from json;
