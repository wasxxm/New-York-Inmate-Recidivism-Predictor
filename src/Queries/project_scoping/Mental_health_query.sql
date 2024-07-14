

-- Get DISTINCT JOIDs
-- note, there are around 1mil distinct joids

WITH distinct_joids AS(
SELECT DISTINCT jc.joid 
FROM raw.jocojococlient AS jc
)


-- GET JOIDs corresponding to JCMHC only with FK
, joids_JCMHC AS (
SELECT DISTINCT joid, CAST (sourceid AS INTEGER) AS sourceid
FROM raw.jocojococlient
WHERE SOURCE  = 'jocoJCMHCDemographics.PATID'
)

-- Count number of Mental health (MH) admissions by patid
, ADMIT_COUNT AS (
SELECT 
        -- admission data
        count( DISTINCT a.admit_date) AS count_admit_date,
        --patient data
        a.patid   
FROM cleaned.jocojcmhcadmissions as a
GROUP BY a.patid
)

-- Get count of mental health calls by patid
, MENTAL_HEALTH_CALLS AS (
SELECT 
        -- admission data
        COUNT( presenting_issue) as mental_health_call_Count,
        --patient data
        patid          
FROM cleaned.jocojcmhccalldetails 
-- Filter calls to only include mental health calls
WHERE presenting_issue in ('SELF HARM', 'BEH PROB/AGGRESSION', 'ANXIETY', 'SUBSTANCE ABUSE', 'DEPRESSION', 'PSYCHOSIS')
GROUP BY patid
)

-- Get insurance status by patid - select most common insurance type per patient
, insurance_status AS (

SELECT DISTINCT ON (patid) patid, payor -- select only 1 row per patid
FROM (
  SELECT patid, payor, COUNT(patid) AS payor_count
  FROM cleaned.jocojcmhcinsurancestatus
  GROUP BY patid, payor
) AS ranked_payors
ORDER BY patid, payor_count DESC, payor

)

-- Get mental health assessment status

, MH_assesment_status AS(
SELECT 
        -- scores
        MAX(dla_score) as MAX_DLA,
        MIN(dla_score) as MIN_DLA,
        MAX(cafas_score) as MAX_CAFAS,
        MIN(cafas_score) as MIN_CAFAS,
        -- id
        patid
FROM 
        cleaned.jocojcmhcoutcomes
GROUP BY patid
)

-- Join all mental health center data together on PAT_ID, then JOID
SELECT 
        -- MH variables
        mhc.mental_health_call_Count, ac.count_admit_date, ins.payor, mhas.MAX_DLA, mhas.MIN_DLA,
        mhas.MAX_CAFAS, mhas.MIN_CAFAS,
        -- IDS
        ac.patid, ids2.joid
        
FROM MENTAL_HEALTH_CALLS  mhc
FULL JOIN ADMIT_COUNT ac ON ac.patid = mhc.patid
FULL JOIN insurance_status ins ON ins.patid = ac.patid
FULL JOIN MH_assesment_status mhas ON mhas.patid = ac.patid
FULL JOIN joids_JCMHC ids1 ON ids1.sourceid = ac.patid
FULL JOIN distinct_joids ids2 ON ids2.joid = ids1.joid;


