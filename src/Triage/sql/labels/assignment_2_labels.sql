with prev_incarc as (
select joid, max(booking_date_full) as last_book
  from cleaned.jocojimsinmatedata
 where booking_date_full between '{as_of_date}'::date - interval '5yr' AND '{as_of_date}'::date
 group by joid
)
, jail_cohort as (
select a.joid 
  from prev_incarc a left join cleaned.jocojimsinmatedata b
       on a.joid = b.joid and a.last_book = b.booking_date_full
 where release_date_full < date('{as_of_date}') and release_date_full is not null
)
, bjmhs as (
select joid, max(bjmhs_referred) as bjmhs_sign
  from cleaned.jocojimsinmatedata
 where 
       booking_date_full BETWEEN '{as_of_date}'::date - interval '5yr' AND '{as_of_date}'::date
 group by joid
)
, lsir as (
select joid, max(case when coalesce(ans_37, ans_38, ans_39, ans_40, ans_41, ans_42, 
                      ans_43, ans_44,ans_45, ans_46, ans_47, ans_48, ans_49, ans_50) is null then NULL
                      when 1 in (ans_37, ans_38, ans_39, ans_40, ans_41, ans_42, ans_43, ans_44,ans_45, 
                      ans_46, ans_47, ans_48, ans_49, ans_50) or 0 in (ans_39, ans_40) then 1
                      else 0 
                 end) as lsir_sign
  from cleaned.jocojimslsirdata
 where 
       assessment_date BETWEEN '{as_of_date}'::date - interval '5yr' AND '{as_of_date}'::date
 group by joid
)
, pta as (
select b.joid, 
       max(case when mnh_flg_8 = 'Y' then 1
            when mnh_flg_8 = 'N' then 0
            else NULL 
       end) as pta_sign
  from raw.jocojimspretrialassessdata left join raw.jocojimsnameindex using(urno)
       left join raw.jocojococlient b on mni_no_0 = b.sourceid and source = 'jocoJIMSNameIndex.MNI_NO_0'
 where 
       create_date BETWEEN '{as_of_date}'::date - interval '5yr' AND '{as_of_date}'::date
 group by b.joid      
)
, jcdhe as (
select distinct joid, 1 as jcdhe_sign
  from cleaned.jocojcdheencounter 
 where
       program ~ 'MENTAL HEALTH' 
       and encounter_date BETWEEN '{as_of_date}'::date - interval '5yr' AND '{as_of_date}'::date
)
, medact as (
select distinct joid, 1 as medact_sign
  from raw.jocomedactincidents a left join cleaned.jocomedactincidents b using(id)
       left join raw.jocojococlient c on c.hash_sourceid = a.hash_rcdid and c.source = 'jocoMEDACTIncidents.RcdID'
 where
       (primaryimpression ~ 'ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS'
       or secondaryimpression ~ 'ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS')
       and primaryimpression ~ '^(?!.*CONCUSSION).*'
       and secondaryimpression ~ '^(?!.*CONCUSSION).*' AND
        b.incidentdate BETWEEN '{as_of_date}'::date - interval '5yr' AND '{as_of_date}'::date
)

, jcmhc_adm as (
select distinct joid, 1 as jcmhc_sign1
  from cleaned.jocojcmhcadmissions
 where
       program_description in ('ADULT MH PROGRAM', 'PEDIATRICS MH PROGRAM', 'EMERGENCY SERVICES', 'MOBILE CRISIS RESPONSE TEAM') AND
       admit_date BETWEEN '{as_of_date}'::date - interval '5yr' AND '{as_of_date}'::date
)

, jcmhc_call as (
select distinct joid, 1 as jcmhc_sign2
  from cleaned.jocojcmhccalldetails
 where 
        suicide_homicide_risk ilike '%%%%violence%%%%' or suicide_homicide_risk ilike '%%%%homicid%%%%' AND
        call_date BETWEEN '{as_of_date}'::date - interval '5yr' AND '{as_of_date}'::date
)
, jail_mh_cohort as 
(select joid
  from jail_cohort left join bjmhs using(joid)
       left join lsir using(joid)
       left join pta using(joid)
       left join jcdhe using(joid)
       left join medact using(joid)
       left join jcmhc_adm using(joid)
       left join jcmhc_call using(joid)
 where 1 in (bjmhs_sign, lsir_sign, pta_sign, jcdhe_sign, medact_sign, jcmhc_sign1, jcmhc_sign2)
)
select joid AS entity_id, 
       max(case 
              WHEN (booking_date_full BETWEEN 
                   date('{as_of_date}') 
                   AND date('{as_of_date}') + interval '{label_timespan}' 
                   )
              AND (coalesce(release_date_full, '2099-01-01 00:00:00') - booking_date_full > '2 weeks'::interval)
              then 1
              else 0 end) AS  outcome
  from cleaned.jocojimsinmatedata 
 where joid in (select joid from jail_mh_cohort)
 group by joid
