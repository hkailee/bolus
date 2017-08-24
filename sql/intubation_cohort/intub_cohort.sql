-- Create a materialized view for the intubation cohort
-- This view only contains icustay_id which are included in the dataset
-- We require:
--    ventdurations

select setseed(0.824);

DROP TABLE IF EXISTS intub_cohort CASCADE;
CREATE TABLE intub_cohort as
-- get services associated with each hospital admission
with serv as
(
  select ie.hadm_id, curr_service as first_service
    , ROW_NUMBER() over (partition by ie.hadm_id order by transfertime DESC) as rn
  from icustays ie
  inner join services se
    on ie.hadm_id = se.hadm_id
    and se.transfertime < ie.intime + interval '1' day
)
, firstvent as
(
  select
    icustay_id, min(starttime) as starttime
  from ventdurations
  group by icustay_id
)
, firsthr as
(
  select
    ie.icustay_id
    , min(ce.charttime) as intime_hr
    , max(ce.charttime) as outtime_hr
  from icustays ie
  inner join admissions adm
    on ie.hadm_id = adm.hadm_id
  inner join chartevents ce
    on ie.icustay_id = ce.icustay_id
    and ce.itemid in (211,220045)
    and ce.valuenum > 0
    and ce.charttime > ie.intime - interval '1' day
    and ce.charttime < ie.outtime + interval '1' day
    and ce.charttime > adm.admittime - interval '1' day
    and ce.charttime < adm.dischtime + interval '1' day
  group by ie.icustay_id
)
, dnr as
(
  select icustay_id
  , min(case
      when value in
      (
          'Comfort Measures','Comfort measures only'
        , 'Do Not Intubate','DNI (do not intubate)','DNR / DNI'
        , 'Do Not Resuscita','DNR (do not resuscitate)','DNR / DNI'
      ) then charttime
    else null end) as dnrtime
  from chartevents
  where itemid in (128, 223758)
  -- exclude rows marked as error
  AND error IS DISTINCT FROM 1
  group by icustay_id
)
, tt as
(
select ie.subject_id, ie.hadm_id, ie.icustay_id
  , ie.intime
  , ie.outtime
  , fhr.intime_hr
  , fhr.outtime_hr
  , dnr.dnrtime
  , case when dnr.dnrtime is not null
      then extract(EPOCH from (dnr.dnrtime - fhr.intime_hr))/60.0/60.0/24.0
      else null end as dnrtime_days

  , fv.starttime as venttime
  , case when fv.starttime is not null
      then extract(EPOCH from (fv.starttime - fhr.intime_hr))/60.0/60.0/24.0
      else null end as venttime_days

  , case
      -- if ventilated, set maxtime to time of ventilation
      when fv.starttime is not null
        then (fv.starttime - fhr.intime_hr)
      -- if not ventilated & if DNR, maxtime is time of DNR
      when dnr.dnrtime is not null and dnr.dnrtime < fhr.outtime_hr
        then (dnr.dnrtime - fhr.intime_hr)
      -- if neither of the above, maxtime is time of discharge
      else (fhr.outtime_hr - fhr.intime_hr)
    end as maxtime


  , se.first_service
  , round(cast(case when fhr.intime_hr > pat.dob + interval '199' year then 91.6
      else extract(EPOCH from (fhr.intime_hr - pat.dob))/60.0/60.0/24.0/365.242 end
      as numeric),2) as age

  , ROW_NUMBER() over (partition by ie.hadm_id order by fhr.intime_hr) as icustay_num
  , RANK() over (partition by ie.subject_id order by adm.admittime) as rn
  , pat.gender

from icustays ie
-- used later to filter out neonates and children
inner join patients pat
  on ie.subject_id = pat.subject_id
-- used later to filter out neonates and children
inner join admissions adm
  on ie.hadm_id = adm.hadm_id
-- used later to filter out patients under certain services
inner join serv se
    on ie.hadm_id = se.hadm_id and se.rn = 1
-- get first time of ventilation
left join firstvent fv
  on ie.icustay_id = fv.icustay_id
-- get first instance of dnr
left join dnr
  on ie.icustay_id = dnr.icustay_id
left join firsthr fhr
  on ie.icustay_id = fhr.icustay_id
-- ORDER BY is important to ensure random() assigns same number to same row
ORDER BY ie.subject_id, ie.hadm_id, ie.icustay_id
)
-- create a random number from 0-1
-- this number will be used to define end time for non-vent patients
, tt2 as
(
  select *
  , random() as random_fraction
  from tt
)
select
    tt2.subject_id, tt2.hadm_id, tt2.icustay_id
  , intime_hr
  , outtime_hr

  -- datatime is EVENT - 3 hours... for example
  -- starttime = datatime - 24 hours
  -- endtime = datatime
  , (case
      when venttime is not null
        then venttime
      -- we ensure that the *earliest* we start is 27 hours after admission
      when maxtime is not null and maxtime >= interval '27' hour
        then ((maxtime - interval '27' hour) * random_fraction) + intime_hr + interval '27' hour
      -- if maxtime <= 27 hours, then starttime is intime_hr
      -- note that exclusion criteria prevent us including DNR data here
      when maxtime is not null
        then intime_hr + interval '27' hour
      else null end) - interval '3' hour
    as datatime

  -- used for exclusions --
  , age
  , gender
  , first_service
  , venttime_days
  , dnrtime_days

  -- exclusion flags --

  -- first icustay
  , case when icustay_num > 1 then 1 else 0 end as exclusion_readmission
  -- remove non-adults
  , case when age < 16 then 1 else 0 end as exclusion_age
  -- not in surgery
  , case when first_service in ('CSURG','NSURG','ORTHO','PSURG','SURG','TSURG','VSURG') then 1 else 0 end
      as exclusion_surgical
  -- not ventilated in the first 27 hours
  , case when coalesce(venttime_days,2) < 27.0/24.0 then 1 else 0 end
      as exclusion_earlyintub
  -- had heart rate data at some time during their stay
  , case when intime_hr is null then 1 else 0 end
      as exclusion_nodata
  , case
      when dnrtime_days < venttime_days then 1
      when dnrtime_days < 27.0/24.0 then 1
    else 0 end
    as exclusion_dnr
, case
    when (outtime_hr - intime_hr) < interval '27' hour then 1
  else 0 end
  as exclusion_shortstay
, case
    when icustay_num > 1 then 1
    when first_service in ('CSURG','NSURG','ORTHO','PSURG','SURG','TSURG','VSURG') then 1
    -- not ventilated in the first 27 hours
    when coalesce(venttime_days,2) < 27.0/24.0 then 1
    when (outtime_hr - intime_hr) < interval '27' hour then 1
    -- had heart rate data at some time during their stay
    when intime_hr is null then 1
    -- not DNR before vent (rare)
    when dnrtime_days < venttime_days then 1
    -- not DNR in the first 27 hours
    when dnrtime_days < 27.0/24.0 then 1
  else 0 end
  as excluded
from tt2;

ALTER TABLE intub_cohort OWNER TO mimic_readonly;
