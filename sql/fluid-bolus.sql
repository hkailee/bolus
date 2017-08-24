DROP TABLE IF EXISTS mp_uo CASCADE;
CREATE TABLE mp_uo AS
with t1 as
(
  select
    co.icustay_id
  , ceil(extract(EPOCH from mv.endtime-co.intime)/60.0/60.0)::smallint as hr
  -- , mv.starttime, mv.endtime
  , mv.itemid
  , case
      when itemid in
      (
      220949, -- Dextrose 5%
      225941, -- D5 1/4NS
      225825, -- D5NS
      225827, -- D5LR
      225823  -- D5 1/2NS
      ) then 'd5'
      when itemid in
      (
      220950, -- Dextrose 10%
      228140, -- Dextrose 20%
      228142, -- Dextrose 40%
      220952, -- Dextrose 50%
      225161, -- NaCl 3% (Hypertonic Saline)
      ) then 'hypertonic crystalloid'
      when itemid in
      (
      220864, --	Albumin 5%	7466 132 7466
      220862, --	Albumin 25%	9851 174 9851
      225174, --	Hetastarch (Hespan) 6%	82 1 82
      225795, --	Dextran 40	38 3 38
      ) then 'colloid'
      when itemid in
      (
        225159 -- NaCl 0.45%
      ) then 'half crystalloid'
      when itemid in
      (
        225158, -- NaCl 0.9%
        225828, -- LR
        225944, -- Sterile Water
        225797, -- Free Water
      ) then 'crystalloid'
    else 'other'
    end as fluid_category
  , mv.amount, mv.amountuom
  , mv.rate, mv.rateuom
  from mp_cohort co
  inner join inputevents_mv mv
  on co.icustay_id = mv.icustay_id
  and mv.itemid in
  (
    -- colloids
    220864, --	Albumin 5%	7466 132 7466
    220862, --	Albumin 25%	9851 174 9851
    225174, --	Hetastarch (Hespan) 6%	82 1 82
    225795, --	Dextran 40	38 3 38
    -- 225943 Solution
    225159, -- NaCl 0.45%
    225161, -- NaCl 3% (Hypertonic Saline)
    225158, -- NaCl 0.9%
    225828, -- LR
    225944, -- Sterile Water
    225797, -- Free Water
    225823, -- D5 1/2NS
    225825, -- D5NS
    225827, -- D5LR
    225941, -- D5 1/4NS
    220949, -- Dextrose 5%
    220950, -- Dextrose 10%
    228140, -- Dextrose 20%
    228142, -- Dextrose 40%
    220952  -- Dextrose 50%
  )
)
select
    icustay_id
  , hr
  , sum(case when fluid_category = 'colloid' then amount else null end) as colloid
  , sum(case when fluid_category = 'crystalloid' then amount else null end) as crystalloid
  , sum(case when fluid_category = 'd5' then amount else null end) as d5
  , sum(case when fluid_category = 'hypertonic crystalloid' then amount else null end) as HypertonicCrystalloid
  , sum(case when fluid_category = 'half crystalloid' then amount else null end) as HalfCrystalloid
from t1
group by t1.icustay_id, t1.hr
order by t1.icustay_id, t1.hr;;
