with all_joids as (
select distinct joid, source, cast(sourceid as integer) as sourceid, hash_sourceid 
  from raw.jocojococlient)

, all_inmates as (
select distinct mni_no, 1 as inmate
  from jocojimsinmatedata)

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
       and secondaryimpression ~ '^(?!.*CONCUSSION).*')
--medact needs to be joined on hash_rcdid and using max() to get only one value for every individual
       
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
 where program ~ 'MENTAL HEALTH')
 
select joid, source, sourceid, inm.mni_no, bjmhs.mni_no, lsir.mni_no, pta.mni_no, 
                               j.patid, hash_sourceid, k.patid, l.patid, m.patid,
                               n.patient_no, hash_sourceid, hash_rcdid,
                               inmate, bjmhs_referred, lsir_mh, pta_mh, medact_mh,
                               jcmhc_client, jcmhc_call, max_dla, min_dla, max_cafas, min_cafas,
                               jcmhc_service, jcdhe_mh
  from all_joids a 
       left join all_inmates inm on a.sourceid = inm.mni_no and source = 'jocoJIMSNameIndex.MNI_NO_0'
       left join bjmhs on a.sourceid = bjmhs.mni_no and source = 'jocoJIMSNameIndex.MNI_NO_0'
       left join lsir on a.sourceid = lsir.mni_no and source = 'jocoJIMSNameIndex.MNI_NO_0'
       left join pta on a.sourceid = pta.mni_no and source = 'jocoJIMSNameIndex.MNI_NO_0'
       left join medact on a.hash_sourceid = medact.hash_rcdid and source = 'jocoMEDACTIncidents.RcdID'
       left join JCMHC_client j on a.sourceid = j.patid and source = 'jocoJCMHCDemographics.PATID'
       left join JCMHC_call k on a.sourceid = k.patid and source = 'jocoJCMHCDemographics.PATID'
       left join MH_assesment_status l on a.sourceid = l.patid and source = 'jocoJCMHCDemographics.PATID'
       left join JCMHC_service m on a.sourceid = m.patid and source = 'jocoJCMHCDemographics.PATID'
       left join JCDHE_MH n on a.sourceid = n.patient_no and source = 'jocoJCDHEEncounter.Patient_no'

