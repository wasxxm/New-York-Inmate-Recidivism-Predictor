
with prev_incarc as (
select joid, max(booking_date_full) as last_book
       --, max(coalesce(release_date_full, '2099-01-01 00:00:00')) as last_release, max(coalesce(release_date_full, '2099-01-01 00:00:00')) - max(booking_date_full) as diff
  from jocojimsinmatedata
 where booking_date_full between date('2012-01-01 00:00:00') and date('2017-01-01 00:00:00')
       and coalesce(release_date_full, '2099-01-01 00:00:00') - booking_date_full > '2 weeks'::interval
 group by joid
 --order by diff desc
)


, jail_cohort as (
select a.joid --, b.joid, a.last_book, b.booking_date_full, release_date_full
  from prev_incarc a left join jocojimsinmatedata b
       on a.joid = b.joid and a.last_book = b.booking_date_full
 where release_date_full < date('2017-01-01 00:00:00') and release_date_full is not null
)

--get bjmhs sign for every joid. At least once bjmhs_referred = 1. We can also filter as booking date < 2018-01-01, it is faster
, bjmhs as (
select joid, max(bjmhs_referred) as bjmhs_sign
  from jocojimsinmatedata
 where --joid in (select joid from jail_cohort) and
       booking_date_full between date('2012-01-01 00:00:00') and date('2017-01-01 00:00:00')
 group by joid
)

--select count(*) from bjmhs where bjmhs_sign = 1


--get lsir sign for every joid. At least once lsir = 1.
, lsir as (
select joid, max(case when coalesce(ans_37, ans_38, ans_39, ans_40, ans_41, ans_42, 
                      ans_43, ans_44,ans_45, ans_46, ans_47, ans_48, ans_49, ans_50) is null then NULL
                      when 1 in (ans_37, ans_38, ans_39, ans_40, ans_41, ans_42, ans_43, ans_44,ans_45, 
                      ans_46, ans_47, ans_48, ans_49, ans_50) or 0 in (ans_39, ans_40) then 1
                      else 0 
                 end) as lsir_sign
  from jocojimslsirdata
 where --joid in (select joid from jail_cohort) and 
       assessment_date between date('2012-01-01 00:00:00') and date('2017-01-01 00:00:00')
 group by joid
)
--select count(*) from lsir where lsir_sign = 1

--get pta sign for every joid. At least once mnh_flg = 'Y'.
, pta as (
select b.joid, 
       max(case when mnh_flg_8 = 'Y' then 1
            when mnh_flg_8 = 'N' then 0
            else NULL 
       end) as pta_sign
  from raw.jocojimspretrialassessdata left join raw.jocojimsnameindex using(urno)
       left join raw.jocojococlient b on mni_no_0 = b.sourceid and source = 'jocoJIMSNameIndex.MNI_NO_0'
 where --b.joid in (select joid from jail_cohort) and
       create_date between date('2012-01-01 00:00:00') and date('2017-01-01 00:00:00')
 group by b.joid      
)

--select count(*) from pta where pta_sign = 1

--get JCDHE MH sign for every joid. At least one encounter with program 'MENTAL HEALTH'.
, jcdhe as (
select distinct joid, 1 as jcdhe_sign
  from jocojcdheencounter 
 where --joid in (select joid from jail_cohort) and
       program ~ 'MENTAL HEALTH' 
       and encounter_date between date('2012-01-01 00:00:00') and date('2017-01-01 00:00:00')
)
--select count(*) from jcdhe

--get MEDACT MH sign for every joid. At least one incident with impression from the listed.
, medact as (
select distinct joid, 1 as medact_sign
  from raw.jocomedactincidents a left join jocomedactincidents b using(id)
       left join raw.jocojococlient c on c.hash_sourceid = a.hash_rcdid and c.source = 'jocoMEDACTIncidents.RcdID'
 where --joid in (select joid from jail_cohort) and
       (primaryimpression ~ 'ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS'
       or secondaryimpression ~ 'ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS')
       and primaryimpression ~ '^(?!.*CONCUSSION).*'
       and secondaryimpression ~ '^(?!.*CONCUSSION).*'
       and b.incidentdate between date('2012-01-01 00:00:00') and date('2017-01-01 00:00:00')
)

--select count(*) joid from medact

--get JCMHC MH sign1 for every joid. At least one admission with program description from the listed below.
, jcmhc_adm as (
select distinct joid, 1 as jcmhc_sign1
  from jocojcmhcadmissions
 where --joid in (select joid from jail_cohort) and
       program_description in ('ADULT MH PROGRAM', 'PEDIATRICS MH PROGRAM', 'EMERGENCY SERVICES', 'MOBILE CRISIS RESPONSE TEAM')
       and admit_date between date('2012-01-01 00:00:00') and date('2017-01-01 00:00:00')
)
--select count(*) from jcmhc_adm

--get JCMHC MH sign2 for every joid. At least one call to JCMHC with homicide/violence risk.
, jcmhc_call as (
select distinct joid, 1 as jcmhc_sign2
  from jocojcmhccalldetails
 where --joid in (select joid from jail_cohort) and
       suicide_homicide_risk ilike '%violence%' or suicide_homicide_risk ilike '%homicid%'
       and call_date between date('2012-01-01 00:00:00') and date('2017-01-01 00:00:00')
)
--select count(*) from jcmhc_call

--get the final cohort with MH signs (any of the MH criteria = 1). 4126 joids
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
--select count(joid) from jail_mh_cohort

,labels as (
select a.joid as cohort_joid, 
       max(case 
              when booking_date_full between date('2017-01-01 00:00:00') and date('2017-07-01 00:00:00') then 1
              when booking_date_full is null then null
              else 0 end) as reincarcerated_label
  from jail_mh_cohort a left join jocojimsinmatedata b on a.joid = b.joid
 group by a.joid
)
select count(*) from labels where reincarcerated_label = 1