
with co as
(
  select
      icustay_id
      , ceil(extract(EPOCH from (datatime - intime_hr) - interval '27' hour)/60.0/60.0)::smallint as hr_start
      , ceil(extract(EPOCH from (datatime - intime_hr) - interval '3' hour)/60.0/60.0)::smallint as hr_end
  from intub_cohort
)
, gcs_stg as
(
  select gs.*
  , ROW_NUMBER() over (PARTITION BY icustay_id ORDER BY hr) as rn_asc
  , ROW_NUMBER() over (PARTITION BY icustay_id ORDER BY hr DESC) as rn_desc
  from co
  inner join mp_gcs gs
    on co.icustay_id = gs.icustay_id
    and co.hr_start <= gs.hr
    and co.hr_end >= gs.hr
    and co.gcs is not null
)
, gcs as
(
  select
    co.icustay_id
    , min(gcs) as gcs_min
    , max(gcs) as gcs_max
    , min(case when rn_asc = 1 then gcs else null end) as gcs_first
    , max(case when rn_desc = 1 then gcs else null end) as gcs_last
  from gcs_stg
)
, bg_first_last as
(
  select
      co.icustayid
    , co.hr_start
    , co.hr_end
    , hr
    , sum(case when so2 is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_so2
    , sum(case when po2 is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_po2
    , sum(case when pco2 is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_pco2
    , sum(case when aado2_calc is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_aado2_calc
    , sum(case when pao2fio2ratio is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_pao2fio2ratio
    , sum(case when ph is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_ph
    , sum(case when baseexcess is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_baseexcess
    , sum(case when bicarbonate is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_bicarbonate
    , sum(case when totalco2 is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_totalco2
    , sum(case when hematocrit is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_hematocrit
    , sum(case when hemoglobin is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_hemoglobin
    , sum(case when carboxyhemoglobin is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_carboxyhemoglobin
    , sum(case when methemoglobin is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_methemoglobin
    , sum(case when chloride is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_chloride
    , sum(case when calcium is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_calcium
    , sum(case when temperature is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_temperature
    , sum(case when potassium is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_potassium
    , sum(case when sodium is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_sodium
    , sum(case when lactate is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_lactate
    , sum(case when glucose is null then 0 else 1 end) over (partition by icustay_id order by hr) as grp_glucose

    , so2
    , po2
    , pco2
    , aado2_calc
    , pao2fio2ratio
    , ph
    , baseexcess
    , bicarbonate
    , totalco2
    , hematocrit
    , hemoglobin
    , carboxyhemoglobin
    , methemoglobin
    , chloride
    , calcium
    , temperature
    , potassium
    , sodium
    , lactate
    , glucose
  from mp_bg_art
)
, bg_fl as
(
  select icustay_id, hr_start, hr_end, hr
    , max(so2) over (partition by icustay_id, grp_so2) as so2
    , max(po2) over (partition by icustay_id, grp_po2) as po2
    , max(pco2) over (partition by icustay_id, grp_pco2) as pco2
    , max(aado2_calc) over (partition by icustay_id, grp_aado2_calc) as aado2_calc
    , max(pao2fio2ratio) over (partition by icustay_id, grp_pao2fio2ratio) as pao2fio2ratio
    , max(ph) over (partition by icustay_id, grp_ph) as ph
    , max(baseexcess) over (partition by icustay_id, grp_baseexcess) as baseexcess
    , max(bicarbonate) over (partition by icustay_id, grp_bicarbonate) as bicarbonate
    , max(totalco2) over (partition by icustay_id, grp_totalco2) as totalco2
    , max(hematocrit) over (partition by icustay_id, grp_hematocrit) as hematocrit
    , max(hemoglobin) over (partition by icustay_id, grp_hemoglobin) as hemoglobin
    , max(carboxyhemoglobin) over (partition by icustay_id, grp_carboxyhemoglobin) as carboxyhemoglobin
    , max(methemoglobin) over (partition by icustay_id, grp_methemoglobin) as methemoglobin
    , max(chloride) over (partition by icustay_id, grp_chloride) as chloride
    , max(calcium) over (partition by icustay_id, grp_calcium) as calcium
    , max(temperature) over (partition by icustay_id, grp_temperature) as temperature
    , max(potassium) over (partition by icustay_id, grp_potassium) as potassium
    , max(sodium) over (partition by icustay_id, grp_sodium) as sodium
    , max(lactate) over (partition by icustay_id, grp_lactate) as lactate
    , max(glucose) over (partition by icustay_id, grp_glucose) as glucose
  from bg_first_last
)
, bg_first as
(
  select * from bg_fl
  where hr = hr_start
)
, bg_last as
(
  select * from bg_fl
  where hr = hr_end
)
, vi as
(

)
, lab as
(

)
, uo as
(

)
, static as
(
  select * from mp_static_data
)
select
    co.icustay_id

  -- outcome
  , case when co.venttime_days is not null then 1 else 0 end as vent

  -- static variables
  , co.age
  , co.gender

  -- blood gases
, bg.SPECIMEN_first
, bg.AADO2_first
, bg.BASEEXCESS_first
, bg.BICARBONATE_first
, bg.TOTALCO2_first
, bg.CARBOXYHEMOGLOBIN_first
, bg.CHLORIDE_first
, bg.CALCIUM_first
, bg.GLUCOSE_first
, bg.HEMATOCRIT_first
, bg.HEMOGLOBIN_first
, bg.INTUBATED_first
, bg.LACTATE_first
, bg.METHEMOGLOBIN_first
, bg.O2FLOW_first
, bg.FIO2_first
, bg.SO2_first -- OXYGENSATURATION
, bg.PCO2_first
, bg.PEEP_first
, bg.PH_first
, bg.PO2_first
, bg.POTASSIUM_first
, bg.REQUIREDO2_first
, bg.SODIUM_first
, bg.TEMPERATURE_first
, bg.TIDALVOLUME_first
, bg.VENTILATIONRATE_first
, bg.VENTILATOR_first

, bg.SPECIMEN_last
, bg.AADO2_last
, bg.BASEEXCESS_last
, bg.BICARBONATE_last
, bg.TOTALCO2_last
, bg.CARBOXYHEMOGLOBIN_last
, bg.CHLORIDE_last
, bg.CALCIUM_last
, bg.GLUCOSE_last
, bg.HEMATOCRIT_last
, bg.HEMOGLOBIN_last
, bg.INTUBATED_last
, bg.LACTATE_last
, bg.METHEMOGLOBIN_last
, bg.O2FLOW_last
, bg.FIO2_last
, bg.SO2_last -- OXYGENSATURATION
, bg.PCO2_last
, bg.PEEP_last
, bg.PH_last
, bg.PO2_last
, bg.POTASSIUM_last
, bg.REQUIREDO2_last
, bg.SODIUM_last
, bg.TEMPERATURE_last
, bg.TIDALVOLUME_last
, bg.VENTILATIONRATE_last
, bg.VENTILATOR_last

-- gcs
, gcs.GCS_min
, gcs.GCS_max
, gcs.GCS_first
, gcs.GCS_last

-- labs
, labs.ANIONGAP_min
, labs.ANIONGAP_max
, labs.ALBUMIN_min
, labs.ALBUMIN_max
, labs.BANDS_min
, labs.BANDS_max
, labs.BICARBONATE_min
, labs.BICARBONATE_max
, labs.BILIRUBIN_min
, labs.BILIRUBIN_max
, labs.CREATININE_min
, labs.CREATININE_max
, labs.CHLORIDE_min
, labs.CHLORIDE_max
, labs.GLUCOSE_min
, labs.GLUCOSE_max
, labs.HEMATOCRIT_min
, labs.HEMATOCRIT_max
, labs.HEMOGLOBIN_min
, labs.HEMOGLOBIN_max
, labs.LACTATE_min
, labs.LACTATE_max
, labs.PLATELET_min
, labs.PLATELET_max
, labs.POTASSIUM_min
, labs.POTASSIUM_max
, labs.PTT_min
, labs.PTT_max
, labs.INR_min
, labs.INR_max
, labs.PT_min
, labs.PT_max
, labs.SODIUM_min
, labs.SODIUM_max
, labs.BUN_min
, labs.BUN_max
, labs.WBC_min
, labs.WBC_max

, labs.ANIONGAP_first
, labs.ANIONGAP_last
, labs.ALBUMIN_first
, labs.ALBUMIN_last
, labs.BANDS_first
, labs.BANDS_last
, labs.BICARBONATE_first
, labs.BICARBONATE_last
, labs.BILIRUBIN_first
, labs.BILIRUBIN_last
, labs.CREATININE_first
, labs.CREATININE_last
, labs.CHLORIDE_first
, labs.CHLORIDE_last
, labs.GLUCOSE_first
, labs.GLUCOSE_last
, labs.HEMATOCRIT_first
, labs.HEMATOCRIT_last
, labs.HEMOGLOBIN_first
, labs.HEMOGLOBIN_last
, labs.LACTATE_first
, labs.LACTATE_last
, labs.PLATELET_first
, labs.PLATELET_last
, labs.POTASSIUM_first
, labs.POTASSIUM_last
, labs.PTT_first
, labs.PTT_last
, labs.INR_first
, labs.INR_last
, labs.PT_first
, labs.PT_last
, labs.SODIUM_first
, labs.SODIUM_last
, labs.BUN_first
, labs.BUN_last
, labs.WBC_first
, labs.WBC_last


  -- urine output
  , uo.UrineOutput

  -- vital signs
, vi.HeartRate_Min
, vi.HeartRate_Max
, vi.HeartRate_Mean
, vi.HeartRate_first
, vi.HeartRate_last

, vi.SysBP_Min
, vi.SysBP_Max
, vi.SysBP_Mean
, vi.SysBP_first
, vi.SysBP_last

, vi.DiasBP_Min
, vi.DiasBP_Max
, vi.DiasBP_Mean
, vi.DiasBP_first
, vi.DiasBP_last

, vi.MeanBP_Min
, vi.MeanBP_Max
, vi.MeanBP_Mean
, vi.MeanBP_first
, vi.MeanBP_last

, vi.RespRate_Min
, vi.RespRate_Max
, vi.RespRate_Mean
, vi.RespRate_first
, vi.RespRate_last

, vi.TempC_Min
, vi.TempC_Max
, vi.TempC_Mean
, vi.TempC_first
, vi.TempC_last

, vi.SpO2_Min
, vi.SpO2_Max
, vi.SpO2_Mean
, vi.SpO2_first
, vi.SpO2_last

, vi.Glucose_Min
, vi.Glucose_Max
, vi.Glucose_Mean
, vi.Glucose_first
, vi.Glucose_last

  -- comorbidity
, ea.congestive_heart_failure
, ea.pulmonary_circulation

from intub_cohort co
-- The following tables are generated by code within this repository
left join intub_bg bg
  on co.icustay_id = bg.icustay_id
left join intub_gcs gcs
  on co.icustay_id = gcs.icustay_id
left join intub_labs labs
  on co.icustay_id = labs.icustay_id
left join intub_uo uo
  on co.icustay_id = uo.icustay_id
left join intub_vitals vi
  on co.icustay_id = vi.icustay_id
left join elixhauser_ahrq ea
  on co.hadm_id = ea.hadm_id

-- apply exclusions
where co.exclusion_randomstay = 0
  and co.exclusion_age = 0
  and co.exclusion_surgical = 0
  and co.exclusion_earlyintub = 0
  and co.exclusion_nodata = 0
  and co.exclusion_dnr = 0

order by co.icustay_id
