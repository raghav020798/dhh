  --How many sessions are there?
  -- count(distinct concat(fullvisitorid, cast(visitstarttime as string))) is preferred count of sessions,
  -- as it deals with midnight-split-sessions immediately.
  -- It can be used in all cases, even when you use the unnest feature
  -- (which will have an impact on the amount of rows in your table and can result in multiple counts of the same sessions):

SELECT
  COUNT(DISTINCT CONCAT(fullvisitorid, CAST(visitstarttime AS string))) AS sessions
FROM
  `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`;


  --How many sessions does each visitor create?
SELECT
  fullvisitorid,
  COUNT(DISTINCT CONCAT(fullvisitorid, CAST(visitstarttime AS string))) AS sessions
FROM
  `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`
GROUP BY
  fullvisitorid
ORDER BY
  fullvisitorid;


  --How much time does it take on average to reach the order_confirmation screen per session (in minutes)?
SELECT
  avg(timeOnSite)/60 as avg_time_to_order
FROM
  `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export` ga,
  UNNEST(hit) AS h
JOIN
  `dhh-analytics-hiringspace.BackendDataSample.transactionalData` tr
ON
  h.transactionId = tr.frontendOrderId


--By using the GoogleAnalyticsSample data and BackendDataSample tables, analyse
-- how often users tend to change their location in the beginning of their journey (screens
-- like home and listing) versus in checkout and on order placement and demonstrate the
-- the deviation between earlier and later inputs (if any) in terms of coordinates change.
-- Then, using the BackendDataSample table, see if those customers who changed their
-- address ended placing orders and if those orders were delivered successfully, if so, did
-- they match their destination.

select fullvisitorid,
       order_placed,
       order_delivered,
       application_type,
  case when dist_change_in_km > 2 then true else false end as address_changed
  from (
select fullvisitorid, session,application_type,geopointDropoff,
case when geopointDropoff is not null and status_id=24 then true
     when geopointDropoff is null and status_id=24 then false else false end as order_delivered,
ST_DISTANCE(start_loc, end_loc)/1000 as dist_change_in_km,
case when transactionId is not null then true else false end as order_placed
from(
    select session,
      max(fullvisitorid) as fullvisitorid,
      max(application_type) as application_type,
      max(transactionId) as transactionId,
      ANY_VALUE(case when screen in ('home','shop_list') then location end) start_loc,
      ANY_VALUE(case when screen in ('checkout') then location end) end_loc 
      from(
        select  fullvisitorid, session,transactionId,application_type,
        case when transactionId is not null then 'checkout' else screen end as screen,
        ST_GeogPoint(long,lat) as location
        from (
              select concat(fullvisitorid, visitStartTime) as session,
                h.hitNUmber as hitnumber,
                max(fullvisitorid) as fullvisitorid,
                max(h.transactionId) as transactionId,
                max(operatingSystem) as application_type, 
                max(case when cd.index = 11 then cd.value end) screen,
                max(case when cd.index = 18 then CASE WHEN REGEXP_CONTAINS(cd.value, r'[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)') 
                                                 THEN cast( cd.value as float64) else null end else null end) long,
                max(case when cd.index = 19 then CASE WHEN REGEXP_CONTAINS(cd.value, r'[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)') 
                                                 THEN cast( cd.value as float64) else null end else null end) lat
              from `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`,unnest(hit) as h, unnest(h.customDimensions) as cd 
              -- where fullvisitorid = '10000560693664842529' 
              GROUP BY session, hitNUmber
              ) sub
        where lat is not null and long is not null) as sub2
      where screen in ('home','shop_list','checkout')
group by session) as GA

left join `dhh-analytics-hiringspace.BackendDataSample.transactionalData` TR
on TR.frontendOrderId = GA.transactionId) as final
;








