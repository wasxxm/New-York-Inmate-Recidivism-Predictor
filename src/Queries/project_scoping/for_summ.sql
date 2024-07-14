with all_joids as (
select distinct joid, source, cast(sourceid as integer) as sourceid, hash_sourceid 
  from raw.jocojococlient)

, all_inmates as (
select mni_no, 1 as inmate, avg(age) as age, max(sex) as sex, max(race) as race, max(marital_status) as mtl_st
  from jocojimsinmatedata
 group by mni_no)

, bjmhs as (
select mni_no, max(bjmhs_referred) as bjmhs_referred 
  from jocojimsinmatedata
 where bjmhs_referred is not null
 group by mni_no)
 
, lsir as (
select mni_no, max(
       case when 1 in (ans_37, ans_38, ans_39, ans_40, ans_41, ans_42, 
        ans_43, ans_44,ans_45, ans_46, ans_47, ans_48, ans_49, ans_50) 
        or 0 in (ans_39, ans_40) then 1
       else 0
       end) as LSIR_MH
  from jocojimscasedata left join jocojimslsirdata using(hash_case_no) 
 where coalesce(ans_37, ans_38, ans_39, ans_40, ans_41, ans_42, 
       ans_43, ans_44,ans_45, ans_46, ans_47, ans_48, ans_49, ans_50) is not null 
 group by mni_no)
       
, pta as (
select cast(mni_no_0 as integer) as mni_no, case when mnh_flg_8 = 'Y' then 1 else 0 end PTA_MH
  from raw.jocojimsnameindex left join raw.jocojimspretrialassessdata using(urno)
 where mnh_flg_8 = 'Y' or mnh_flg_8 = 'N')
 
, medact as (
select a.hash_rcdid, 1 as medact_MH 
  from raw.jocomedactincidents a full join jocomedactincidents b using(id)
 where (primaryimpression ~ 'ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS'
       or secondaryimpression ~ 'ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS')
       and primaryimpression ~ '^(?!.*CONCUSSION).*'
       and secondaryimpression ~ '^(?!.*CONCUSSION).*'
)
--medact needs to be joined on hash_rcdid and using max() to get only one value for every individual

, medact_counts as (
select joid, count(hash_sourceid) as medact_freq 
  from raw.jocojococlient 
 where source = 'jocoMEDACTIncidents.RcdID'
 group by joid)
 
, JCMHC_dem as (
select patid, race, sex from jocojcmhcdemographics)

       
, JCMHC_client as (
select distinct patid, 1 as JCMHC_client 
  from jocojcmhcadmissions)
  
, JCMHC_call as (
select distinct patid, 1 as JCMHC_call
  from jocojcmhccalldetails
 where presenting_issue in ('SELF HARM', 'BEH PROB/AGGRESSION', 'ANXIETY', 
                            'SUBSTANCE ABUSE', 'DEPRESSION', 'PSYCHOSIS'))
                            
, MH_assesment_status AS(
SELECT 
        patid,
        -- scores
        MAX(dla_score) as MAX_DLA,
        MIN(dla_score) as MIN_DLA,
        MAX(cafas_score) as MAX_CAFAS,
        MIN(cafas_score) as MIN_CAFAS
  FROM 
        cleaned.jocojcmhcoutcomes
 GROUP BY patid)
 
, JCMHC_service as (
select distinct patid, 1 as JCMHC_service 
  from jocojcmhcservices)

, JCDHE_MH as (
select distinct patient_no, 1 as JCDHE_MH 
  from jocojcdheencounter 
 where program ~ 'MENTAL HEALTH'),

inmate_twice as (
select mni_no, 1 as inm_twice
  from (select mni_no, count(booking_date) as cnt
  from jocojimsinmatedata
 group by mni_no
 order by cnt desc) as cnts
 where cnt > 1)
  
select a.joid, inmate, inm_twice, bjmhs_referred, lsir_mh, pta_mh, medact_mh,
             jcmhc_client, jcmhc_call, jcmhc_service, jcdhe_mh, 
             inm.age, coalesce(inm.sex, o.sex), coalesce(inm.race, o.race), mtl_st, medact_freq,
             case when inm_twice = 1 and 1 in (bjmhs_referred, lsir_mh, pta_mh, medact_mh,
             jcmhc_client, jcmhc_call, jcmhc_service, jcdhe_mh) then 1
             else 0 end as target
  from all_joids a 
       left join all_inmates inm on a.sourceid = inm.mni_no and source = 'jocoJIMSNameIndex.MNI_NO_0'
       left join bjmhs on a.sourceid = bjmhs.mni_no and source = 'jocoJIMSNameIndex.MNI_NO_0'
       left join lsir on a.sourceid = lsir.mni_no and source = 'jocoJIMSNameIndex.MNI_NO_0'
       left join pta on a.sourceid = pta.mni_no and source = 'jocoJIMSNameIndex.MNI_NO_0'
       left join medact on a.hash_sourceid = medact.hash_rcdid and source = 'jocoMEDACTIncidents.RcdID'
       left join medact_counts on a.joid = medact_counts.joid and source = 'jocoMEDACTIncidents.RcdID'
       left join JCMHC_client j on a.sourceid = j.patid and source = 'jocoJCMHCDemographics.PATID'
       left join JCMHC_call k on a.sourceid = k.patid and source = 'jocoJCMHCDemographics.PATID'
       left join MH_assesment_status l on a.sourceid = l.patid and source = 'jocoJCMHCDemographics.PATID'
       left join JCMHC_service m on a.sourceid = m.patid and source = 'jocoJCMHCDemographics.PATID'
       left join JCDHE_MH n on a.sourceid = n.patient_no and source = 'jocoJCDHEEncounter.Patient_no'
       left join inmate_twice inm2 on a.sourceid = inm2.mni_no and source = 'jocoJIMSNameIndex.MNI_NO_0'
       left join JCMHC_dem o on a.sourceid = o.patid and source = 'jocoJCMHCDemographics.PATID'
