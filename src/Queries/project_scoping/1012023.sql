with bookings as (
-- define the cohort: incarcerated (booked) within last 5 years and released before today (2013-01-01 - 2018-01-01)
select distinct (joid)
from jocojimsinmatedata
where booking_date_full > DATE('2013-01-01 00:00:00') and release_date_full < DATE('2018-01-01 00:00:00')
)

, bjmhs01 as (
-- MH criteria - BJMHS: if during last 5 years, at least once bjmhs_referred = 1 then 1, if only non-null is 0, then 0, if no info, then null
-- May be we can get the latest result and keep that as the final result
select joid, max(bjmhs_referred) as bjmhs_t01
from jocojimsinmatedata
where booking_date_full between date('2013-01-01 00:00:00') and date('2018-01-01 00:00:00')
group by joid
)

-- same latest result for LSIR
, lsir01 as (
-- MH criteria - LSIR: if during last 5 years, at least once LSIR criteria (as described on github) = 1 then 1, if only non-null is 0, then 0, if no info, then null
select joid, max(case 
                when coalesce(ans_37, ans_38, ans_39, ans_40, ans_41, ans_42, 
                     ans_43, ans_44,ans_45, ans_46, ans_47, ans_48, ans_49, ans_50) is null then NULL
                when 1 in (ans_37, ans_38, ans_39, ans_40, ans_41, ans_42, ans_43, ans_44,ans_45, 
                     ans_46, ans_47, ans_48, ans_49, ans_50) or 0 in (ans_39, ans_40) then 1
                else 0 
                end) as lsir_t01
  from bookings left join raw.jocojococlient b using(joid)
                left join jocojimscasedata c on cast(b.sourceid as integer) = c.mni_no 
                left join jocojimslsirdata using(hash_case_no)
 where assessment_date between date('2013-01-01 00:00:00') and date('2018-01-01 00:00:00')
 group by joid
)

-- same latest result for PTA01
, pta01 as (
-- MH criteria - PTA: if during last 5 years, at least once mh_flag = Y(es) then 1, if only non-null is N(o), then 0, if no info, then null

select joid, max(pta_MH) as pta_t01
  from bookings b left join raw.jocojococlient c using(joid)
  left join (select cast(mni_no_0 as integer) as mni_no, 
                    case when mnh_flg_8 = 'Y' then 1
                         when mnh_flg_8 = 'N' then 0
                         else NULL end as pta_MH  
               from raw.jocojimsnameindex left join raw.jocojimspretrialassessdata using(urno)
               where create_date between date('2013-01-01 00:00:00') and date('2018-01-01 00:00:00')) as mnis
               on cast(c.sourceid as integer) = mnis.mni_no
 group by joid
)

, medact01 as (
-- MH criteria - MEDACT incidents: if during last 5 years, at least one incident with primarry or secondary impression includes "ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS", then 1, otherwise null
select joid, max(medact_MH) as medactMH_t01
  from bookings a left join raw.jocojococlient b using(joid)
       left join 
       (select a.hash_rcdid, 1 as medact_MH
          from raw.jocomedactincidents a full join jocomedactincidents b using(id)
         where (primaryimpression ~ 'ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS'
                or secondaryimpression ~ 'ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS')
                and primaryimpression ~ '^(?!.*CONCUSSION).*'
                and secondaryimpression ~ '^(?!.*CONCUSSION).*'
                and b.incidentdate between date('2013-01-01 00:00:00') and date('2018-01-01 00:00:00')) as medact
        on hash_sourceid = hash_rcdid and source = 'jocoMEDACTIncidents.RcdID'
 group by joid
)

, jcdhe01 as (
-- MH criteria - JCDHE encounters: if during last 5 years, at least one encounter where program is 'Mental Health', then 1, otherwise null
select joid, max(jcdhe) as jcdheMH_t01
  from bookings a left join raw.jocojococlient b using(joid)
       left join (select patient_no, 1 as jcdhe 
                    from jocojcdheencounter 
                   where program ~ 'MENTAL HEALTH' 
                   and encounter_date between date('2013-01-01 00:00:00') 
                   and date('2018-01-01 00:00:00')) as jcdhe_MH
       on cast(sourceid as integer) = patient_no and source = 'jocoJCDHEEncounter.Patient_no'
 group by joid 
)

, jcmhc01 as (
-- MH criteria - JCMHC admissions: if during last 5 years admitted to JCMHC, then 1, otherwise null

select joid, max(jcmhc_MH) as mhc_t01
  from bookings a left join raw.jocojococlient b using(joid)
       left join (select patid, 1 as jcmhc_MH
                    from jocojcmhcadmissions
                  where admit_date between date('2013-01-01 00:00:00') and date('2018-01-01 00:00:00')) as c
            on cast(sourceid as integer) = patid and source = 'jocoJCMHCDemographics.PATID'
       
 group by joid 
)

--get all MH01 criteria for every joid from cohort
, pre_final01 as (
select joid, bjmhs_t01, lsir_t01, pta_t01, medactMH_t01, jcdheMH_t01, mhc_t01
 from bookings left join bjmhs01 using(joid)
               left join lsir01 using(joid)
               left join pta01 using(joid)
               left join medact01 using(joid)
               left join jcdhe01 using(joid)
               left join jcmhc01 using(joid)
)

--aggregate MH_sign01 as: if 1 in any of the criteria, then 1, if only known non-null value is 0, then 0, else null
, MH_sign01 as (
select joid, greatest(bjmhs_t01, lsir_t01, pta_t01, medactMH_t01, jcdheMH_t01, mhc_t01) as MH_sign01
  from pre_final01
)

, bjmhs12 as (
-- MH criteria - BJMHS: if in 6 months from now, at least once bjmhs_referred = 1 then 1, if only non-null is 0, then 0, if no info, then null
select joid, max(bjmhs_referred) as bjmhs_t12
from jocojimsinmatedata
where booking_date_full between date('2018-01-01 00:00:00') and date('2018-07-01 00:00:00')
group by joid
)

, lsir12 as (
-- MH criteria - LSIR: if in 6 months from now, at least once LSIR criteria (as described on github) = 1 then 1, if only non-null is 0, then 0, if no info, then null
select joid, max(case 
                when coalesce(ans_37, ans_38, ans_39, ans_40, ans_41, ans_42, 
                     ans_43, ans_44,ans_45, ans_46, ans_47, ans_48, ans_49, ans_50) is null then NULL
                when 1 in (ans_37, ans_38, ans_39, ans_40, ans_41, ans_42, ans_43, ans_44,ans_45, 
                     ans_46, ans_47, ans_48, ans_49, ans_50) or 0 in (ans_39, ans_40) then 1
                else 0 
                end) as lsir_t12
  from bookings left join raw.jocojococlient b using(joid)
                left join jocojimscasedata c on cast(b.sourceid as integer) = c.mni_no 
                left join jocojimslsirdata using(hash_case_no)
 where assessment_date between date('2018-01-01 00:00:00') and date('2018-07-01 00:00:00')
 group by joid
)

, pta12 as (
-- MH criteria - PTA: if in 6 months from now, at least once mh_flag = Y(es) then 1, if only non-null is N(o), then 0, if no info, then null

select joid, max(pta_MH) as pta_t12
  from bookings b left join raw.jocojococlient c using(joid)
  left join (select cast(mni_no_0 as integer) as mni_no, 
                    case when mnh_flg_8 = 'Y' then 1
                         when mnh_flg_8 = 'N' then 0
                         else NULL end as pta_MH  
               from raw.jocojimsnameindex left join raw.jocojimspretrialassessdata using(urno)
               where create_date between date('2018-01-01 00:00:00') and date('2018-07-01 00:00:00')) as mnis
               on cast(c.sourceid as integer) = mnis.mni_no
 group by joid
)

, medact12 as (
-- MH criteria - MEDACT incidents: if in 6 months from now, at least one incident with primarry or secondary impression includes "ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS", then 1, otherwise null
select joid, max(medact_MH) as medactMH_t12
  from bookings a left join raw.jocojococlient b using(joid)
       left join 
       (select a.hash_rcdid, 1 as medact_MH
          from raw.jocomedactincidents a full join jocomedactincidents b using(id)
         where (primaryimpression ~ 'ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS'
                or secondaryimpression ~ 'ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS')
                and primaryimpression ~ '^(?!.*CONCUSSION).*'
                and secondaryimpression ~ '^(?!.*CONCUSSION).*'
                and b.incidentdate between date('2018-01-01 00:00:00') and date('2018-07-01 00:00:00')) as medact
        on hash_sourceid = hash_rcdid and source = 'jocoMEDACTIncidents.RcdID'
 group by joid
)

, jcdhe12 as (
-- MH criteria - JCDHE encounters: if in 6 months from now, at least one encounter where program is 'Mental Health', then 1, otherwise null
select joid, max(jcdhe) as jcdheMH_t12
  from bookings a left join raw.jocojococlient b using(joid)
       left join (select patient_no, 1 as jcdhe 
                    from jocojcdheencounter 
                   where program ~ 'MENTAL HEALTH' 
                   and encounter_date between date('2018-01-01 00:00:00') and date('2018-07-01 00:00:00')) as jcdhe_MH
       on cast(sourceid as integer) = patient_no and source = 'jocoJCDHEEncounter.Patient_no'
 group by joid 
)

, jcmhc12 as (
-- MH criteria - JCMHC admissions: if in 6 months from now admitted to JCMHC, then 1, otherwise null

select joid, max(jcmhc_MH) as mhc_t12
  from bookings a left join raw.jocojococlient b using(joid)
       left join (select patid, 1 as jcmhc_MH
                    from jocojcmhcadmissions
                  where admit_date between date('2018-01-01 00:00:00') and date('2018-07-01 00:00:00')) as c
            on cast(sourceid as integer) = patid and source = 'jocoJCMHCDemographics.PATID'
       
 group by joid 
)

--get all MH12 criteria for every joid from cohort
, pre_final12 as (
select joid, bjmhs_t12, lsir_t12, pta_t12, medactMH_t12, jcdheMH_t12, mhc_t12
 from bookings left join bjmhs12 using(joid)
               left join lsir12 using(joid)
               left join pta12 using(joid)
               left join medact12 using(joid)
               left join jcdhe12 using(joid)
               left join jcmhc12 using(joid)
)

--aggregate MH_sign12 as: if 1 in any of the criteria, then 1, if only known non-null value is 0, then 0, else null
, MH_sign12 as (
select joid, greatest(bjmhs_t12, lsir_t12, pta_t12, medactMH_t12, jcdheMH_t12, mhc_t12) as MH_sign12
  from pre_final12
)

select *
from MH_sign01 full join MH_sign12 using(joid)