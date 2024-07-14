
-- t0: 2013-01-01
-- t1: 2018-01-01
-- t2: 2018-07-01

-- Cohort: People who have 1+ incarceration(s) between t0 and t1 (minimum 2 weeks) and currently not incarcerated
-- Outcome = 1: Who will go back to jail for at least 2 weeks between t1 and t2 AND has mental health problems?


------- Get all JOIDS in entire data system ------- 
WITH all_JOIDs AS 
(
        SELECT DISTINCT(joid) as JOID
          FROM raw.JocoJocoClient
)
---------------------------------------------- 

------- Get number of incarcerations > 2 weeks between t0 and t1 ------- 
, Incarceration_Count_1 AS
(
        -- Get incarcerations that started between time0 and time1 with duration > MinDuration
        SELECT
                JOID,
                COUNT(JOID) as Incarceration_Count_1    
        FROM cleaned.JocoJIMSInmateData current_book
        WHERE 
             booking_date_full < DATE('2018-01-01 00:00:00')
             AND booking_date_full > DATE('2013-01-01 00:00:00')
             AND (
                 ((release_date_full - booking_date_full) > '2 weeks'::interval )
                 OR 
                 release_date_full IS NULL
                 )
        GROUP BY joid
)
----------------------------------------------

------- Get number of incarcerations between t1 and t2 ------- 
, Incarceration_Count_2 AS
(
        -- Get incarcerations that started between time0 and time1 with duration > MinDuration
        SELECT
                JOID,
                COUNT(JOID) as Incarceration_Count_2    
        FROM cleaned.JocoJIMSInmateData current_book
        WHERE 
             booking_date_full < DATE('2018-07-01 00:00:00')
             AND booking_date_full > DATE('2018-01-01 00:00:00')
             AND (
                 ((release_date_full - booking_date_full) > '2 weeks'::interval )
                 OR 
                 release_date_full IS NULL
                 )
        GROUP BY joid
)
----------------------------------------------



------- Get Current incarceration status at t1 ------- 
, Incarceration_Status AS (
        SELECT
                JOID, MAX(currently_incarcerated) as currently_incarcerated
                FROM(
                     SELECT
                        JOID,
                        1 AS currently_incarcerated,
                        ROW_NUMBER() OVER (PARTITION BY joid ORDER BY booking_date_full) AS bookings_counter       
                        FROM cleaned.JocoJIMSInmateData
                        --Filter to only keep current incarcerations
                        WHERE 
                                (booking_date_full < DATE('2018-01-01 00:00:00')
                                AND  ( 
                                      release_date_full > DATE('2018-01-01 00:00:00') 
                                      OR release_date_full IS NULL)
                                )
                  ) current_incarcerations
                  WHERE bookings_counter=1
                  GROUP BY JOID
 )        

----------------------------------------------      
                  
------- Get Results of BJHMS Survey in t0-t1 period ------- 
, BJMHS1 AS
(
        SELECT
                joid, 
                max(bjmhs_referred) as bjmhs_t1
        from cleaned.jocojimsinmatedata
        where booking_date_full between date('2013-01-01 00:00:00') and date('2018-01-01 00:00:00')
        group by joid
)

----------------------------------------------

------- Get Results of BJHMS Survey in t1-t2 period ------- 
, BJMHS2 AS
(
        SELECT
                joid, 
                max(bjmhs_referred) as bjmhs_t2
        from cleaned.jocojimsinmatedata
        where booking_date_full between date('2018-01-01 00:00:00') and date('2018-07-01 00:00:00')
        group by joid
)
----------------------------------------------

------- Get Results of LSIR t0-t1 ------- 
, LSIR1 AS
(
        select joid, max(case 
                        when coalesce(ans_37, ans_38, ans_39, ans_40, ans_41, ans_42, 
                             ans_43, ans_44,ans_45, ans_46, ans_47, ans_48, ans_49, ans_50) is null then NULL
                        when 1 in (ans_37, ans_38, ans_39, ans_40, ans_41, ans_42, ans_43, ans_44,ans_45, 
                             ans_46, ans_47, ans_48, ans_49, ans_50) or 0 in (ans_39, ans_40) then 1
                        else 0 
                        end) as lsir_t1
          from cleaned.JocoJIMSInmateData left join raw.jocojococlient b using(joid)
                        left join cleaned.jocojimscasedata c on cast(b.sourceid as integer) = c.mni_no 
                        left join cleaned.jocojimslsirdata using(hash_case_no)
         where assessment_date between date('2013-01-01 00:00:00') and date('2018-01-01 00:00:00')
         group by joid
)
----------------------------------------------

------- Get Results of LSIR t1-t2 ------- 
, LSIR2 AS
(
        select joid, max(case 
                        when coalesce(ans_37, ans_38, ans_39, ans_40, ans_41, ans_42, 
                             ans_43, ans_44,ans_45, ans_46, ans_47, ans_48, ans_49, ans_50) is null then NULL
                        when 1 in (ans_37, ans_38, ans_39, ans_40, ans_41, ans_42, ans_43, ans_44,ans_45, 
                             ans_46, ans_47, ans_48, ans_49, ans_50) or 0 in (ans_39, ans_40) then 1
                        else 0 
                        end) as lsir_t2
          from cleaned.jocojimsinmatedata left join raw.jocojococlient b using(joid)
                        left join cleaned.jocojimscasedata c on cast(b.sourceid as integer) = c.mni_no 
                        left join cleaned.jocojimslsirdata using(hash_case_no)
         where assessment_date between date('2018-01-01 00:00:00') and date('2018-07-01 00:00:00')
         group by joid
)
----------------------------------------------

------- Get Results of PTA t0-t1 ------- 
, PTA1 AS(
        -- MH criteria - PTA: if during last 5 years, at least once mh_flag = Y(es) then 1, 
        -- if only non-null is N(o), then 0, if no info, then null
        select joid, max(pta_MH) as pta_t1
          from cleaned.JocoJIMSInmateData b left join raw.jocojococlient c using(joid)
          left join (select cast(mni_no_0 as integer) as mni_no, 
                            case when mnh_flg_8 = 'Y' then 1
                                 when mnh_flg_8 = 'N' then 0
                                 else NULL end as pta_MH  
                       from raw.jocojimsnameindex left join raw.jocojimspretrialassessdata using(urno)
                       where create_date between date('2013-01-01 00:00:00') and date('2018-01-01 00:00:00')) as mnis
                       on cast(c.sourceid as integer) = mnis.mni_no
         group by joid
)

----------------------------------------------

------- Get Results of PTA t1-t2 ------- 
, PTA2 AS(
        -- MH criteria - PTA: if during last 5 years, at least once mh_flag = Y(es) then 1, 
        -- if only non-null is N(o), then 0, if no info, then null
        select joid, max(pta_MH) as pta_t2
          from cleaned.JocoJIMSInmateData b left join raw.jocojococlient c using(joid)
          left join (select cast(mni_no_0 as integer) as mni_no, 
                            case when mnh_flg_8 = 'Y' then 1
                                 when mnh_flg_8 = 'N' then 0
                                 else NULL end as pta_MH  
                       from raw.jocojimsnameindex left join raw.jocojimspretrialassessdata using(urno)
                       where create_date between date('2018-01-01 00:00:00') and date('2018-07-01 00:00:00')) as mnis
                       on cast(c.sourceid as integer) = mnis.mni_no
         group by joid
)
----------------------------------------------

------- Get Results of any MEDACT Incidents in 5 years t0-t1 ------- 
, medact1 as (
-- MH criteria - MEDACT incidents: if during last 5 years, at least one incident with primarry or secondary impression includes "ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS", then 1, otherwise null
        select raw.joid, 
               max(medact_MH) as medactMH_t1
               FROM raw.jocojococlient raw
               left join 
                       (select a.hash_rcdid,
                          1 as medact_MH
                          from raw.jocomedactincidents a 
                                full join cleaned.jocomedactincidents b using(id)
                         where (primaryimpression ~ 'ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS'
                                or secondaryimpression ~ 'ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS')
                                and primaryimpression ~ '^(?!.*CONCUSSION).*'
                                and secondaryimpression ~ '^(?!.*CONCUSSION).*'
                                and b.incidentdate between date('2013-01-01 00:00:00') and date('2018-01-01 00:00:00'))
                as medact
                on hash_sourceid = hash_rcdid and source = 'jocoMEDACTIncidents.RcdID'
                group by joid
)

----------------------------------------------

------- Get Results of any MEDACT Incidents in t1-t2 ------- 
, medact2 as (
-- MH criteria - MEDACT incidents: if during last 5 years, at least one incident with primarry or secondary impression includes "ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS", then 1, otherwise null
        select raw.joid, 
               max(medact_MH) as medactMH_t2
               FROM raw.jocojococlient raw
               left join 
                       (select a.hash_rcdid,
                          1 as medact_MH
                          from raw.jocomedactincidents a 
                                full join cleaned.jocomedactincidents b using(id)
                         where (primaryimpression ~ 'ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS'
                                or secondaryimpression ~ 'ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS')
                                and primaryimpression ~ '^(?!.*CONCUSSION).*'
                                and secondaryimpression ~ '^(?!.*CONCUSSION).*'
                                and b.incidentdate between date('2018-01-01 00:00:00') and date('2018-07-01 00:00:00'))
                as medact
                on hash_sourceid = hash_rcdid and source = 'jocoMEDACTIncidents.RcdID'
                group by joid
)

----------------------------------------------

--Missing: JCDHE encounters, JCMHC admissions

------- AGGREGATE Mental Health Signs ------- 

--get all MH criteria for every joid from cohort
, all_MH as (
        select joid, 
               COALESCE(bjmhs_t1, 0) AS bjmhs_t1, 
               COALESCE(lsir_t1, 0) AS lsir_t1,
               COALESCE(pta_t1,0) AS pta_t1,
               COALESCE(medactMH_t1,0) AS medactMH_t1,
               -- COALESCE(jcdheMH_t1,0),
               -- COALESCE(mhc_t1, 0)
               
               COALESCE(bjmhs_t2, 0) AS bjmhs_t2, 
               COALESCE(lsir_t2, 0) AS lsir_t2,
               COALESCE(pta_t2,0) AS pta_t2,
               COALESCE(medactMH_t2,0) AS medactMH_t2
               -- COALESCE(jcdheMH_t1,0),
               -- COALESCE(mhc_t1, 0)       
                       
         from BJMHS1 
                 FULL JOIN PTA1 using(joid)
                 FULL JOIN MEDACT1 using(joid)
                 FULL JOIN LSIR1 using(JOID)
                 FULL JOIN BJMHS2 using(joid)
                 FULL JOIN PTA2 using(joid)
                 FULL JOIN MEDACT2 using(joid)
                 FULL JOIN LSIR2 using(JOID)
)

, MH_aggregated as (
        SELECT
                joid, 
                GREATEST(bjmhs_t1, 
                        lsir_t1, 
                        pta_t1, 
                        medactMH_t1
                        --jcdheMH_t1, 
                        --mhc_t1
                        ) as MH_sign1,
                GREATEST(bjmhs_t2, 
                        lsir_t2, 
                        pta_t2, 
                        medactMH_t2
                        --jcdheMH_t2, 
                        --mhc_t2
                        ) as MH_sign2
          from all_MH

)

--------------------------------

, pre_final AS (
SELECT all_joids.joid, 
       COALESCE(incarceration_count_1, 0) AS incarceration_count_1,
       COALESCE(incarceration_count_2, 0) AS incarceration_count_2,    
       COALESCE(currently_incarcerated, 0) AS  currently_incarcerated,
       MH_sign1,
       MH_sign2

       
  FROM all_JOIDs
  LEFT JOIN Incarceration_Count_1 IC1
        ON all_JOIDs.joid = IC1.joid
  LEFT JOIN Incarceration_Count_2 IC2
        ON all_JOIDs.joid = IC2.joid
  LEFT JOIN Incarceration_Status IncS
        ON all_JOIDS.joid = IncS.joid
  LEFT JOIN MH_aggregated
        ON all_JOIDS.joid = MH_aggregated.joid
)

--------------------------------

, cohort_and_outcome AS
(

SELECT 
        JOID,
        incarceration_count_1,
        incarceration_count_2,
        mh_sign1,
        mh_sign2,
        
        CASE
                WHEN incarceration_count_1>0 AND currently_incarcerated=0 
                        THEN 1 
                ELSE 
                        0 
        END AS COHORT,
        CASE
                WHEN incarceration_count_1>0 AND currently_incarcerated=0  AND incarceration_count_2>0
                        AND (mh_sign1=1 OR mh_sign2 =1)
                        THEN 1
                ELSE
                        0
        END AS OUTCOME,
        
        CASE WHEN incarceration_count_1>0 THEN 1 ELSE 0 END AS incarcerated_1,
        CASE WHEN incarceration_count_2>0 THEN 1 ELSE 0 END AS incarcerated_2     
           
  FROM pre_final
)

--------------------------------

-- SummaryStats --

SELECT 
        COUNT(JOID) AS Num_Residents,
        SUM(COHORT) AS Num_Cohort,
        SUM(OUTCOME) As Num_Outcome_is_1,
        SUM(mh_sign1) AS Num_MH_1,
        SUM(mh_sign2) AS Num_MH_2,
        SUM(incarcerated_1) AS Num_Incarcerated_1,
        SUM(incarcerated_2) AS Num_Incarcerated_2
        
FROM cohort_and_outcome



    