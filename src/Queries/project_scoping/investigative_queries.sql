-- Reference: 

-- sources in JocoJocoClient:

-- SOURCES: 
-- jocoJCDHEEncounter.Patient_no
-- jocoMEDACTIncidents.RcdID
-- jocoJIMSNameIndex.MNI_NO_0
-- jocoJCMHCDemographics.PATID
-- jocoAIMSBookings.CFN
-- jocoKDOCDemographics.DOCNum
-- jocoPDArrests.NAME_ID


------------------------------------------------------------------------------------------------
-- QUERY: How many times does a JOID appear in Raw.JocoJocoClient? 

WITH ID_COUNTS AS (
SELECT COUNT( joid) as ID_COUNT from raw.jocojococlient
GROUP BY joid
)
SELECT ID_COUNT, COUNT(ID_COUNT) as FREQ from ID_COUNTS
GROUP BY ID_COUNT
ORDER BY ID_COUNT

-- Answer: Most JOID's appear only 1-10 times, few outliers of 10-200 times


-----------------------------------
-----------------------------------
--                               --
--                               --
--      DATA LINKAGE             -- 
--                               --
--                               --
-----------------------------------
-----------------------------------

------------------
--  QUERY: Retrieve information about an individual's health encounter data
-- Data source: Johnson County Department of Health encounter data (JCDHE)
-- JOIN JCDHE Encounter data with JOID's --
------------------



-- Create list of JOID that match encounter data
with JCDHE_encounter AS (
SELECT joid, CAST (sourceid AS INTEGER) AS sourceid
FROM raw.jocojococlient
WHERE SOURCE  = 'jocoJCDHEEncounter.Patient_no'
)
--Join data to encounter data
SELECT 
       -- Encounter data
       e.encounter_date, e.joco_resident, program, provider_hash,  e.respon_first_hash, e.respon_last_hash,
       -- Unknown data
       cosite, cnt, rn,
       -- Person data
       IDS.joid, patient_no, first_name_hash,last_name_hash, date_of_birth, ssn_hash, address_hash, city, state, zip_code, race, sex
FROM cleaned.jocojcdheencounter as e
--JOIN
FULL JOIN JCDHE_encounter AS IDS
ON e.patient_no = IDS.sourceid;

------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------



------------------
--QUERY: Retrieve information about an individual's involvement with Johnson County Mental Health Center
-- Data source: Johnson County Department of Health encounter data (JCMHC)
-- JOIN JCMHC Encounter data with JOID's --
------------------



-- JCMHC data

-------------------- Admission's data -------------------

-- JOID's
with JCDHE_encounter AS (
SELECT joid, CAST (sourceid AS INTEGER) AS sourceid
FROM raw.jocojococlient
WHERE SOURCE  = 'jocoJCMHCDemographics.PATID'
)
--Join data to admissions data
SELECT 
        -- admission data
        a.admit_date, a.program_description,
        -- other data
        a.sourcesystem,
        --patient data
        a.patid, IDS.joid
FROM cleaned.jocojcmhcadmissions as a
--JOIN
FULL JOIN JCDHE_encounter AS IDS
ON a.patid = IDS.sourceid



----------------------- Call data -------- 
with JCDHE_encounter AS (
SELECT joid, CAST (sourceid AS INTEGER) AS sourceid
FROM raw.jocojococlient
WHERE SOURCE  = 'jocoJCMHCDemographics.PATID'
)
SELECT 
        -- Call data
        c.call_date, c.call_type, c.presenting_issue, c.suicide_homicide_risk, c.degree_of_risk, c.disposition, c.visit_type, 
        -- Other info
        c.unit, c.status, c.sud_acuity, c.sud_risk_factors, c.sourcesystem,
        --IDS:
        c.patid, IDS.joid
FROM cleaned.jocojcmhccalldetails as c
--JOIN
FULL JOIN JCDHE_encounter AS IDS
ON c.patid = IDS.sourceid;

-------------------- Demographics data -------------------
-- JOID's
with JCDHE_encounter AS (
SELECT joid, CAST (sourceid AS INTEGER) AS sourceid
FROM raw.jocojococlient
WHERE SOURCE  = 'jocoJCMHCDemographics.PATID'
)
SELECT 
        -- other data
        d.referral_source, d.sourcesystem,
        
        --patient data
        --IDS:
        d.patid, IDS.joid, d.hash_ssn, d.hash_fname, d.hash_lname,
        -- other info:
        d.dob, d.race, d.sex,
        -- location info:
        d.hash_address, d.city, d.state, d.zip, d.joco_resident, d.zip_4, d.tract2010id, d.blockgroup2010id, d.block2010id       
FROM cleaned.jocojcmhcdemographics as d
--JOIN
FULL JOIN JCDHE_encounter AS IDS
ON d.patid = IDS.sourceid


-------------------- Diagnoses data -------------------

-- JOID's
with JCDHE_encounter AS (
SELECT joid, CAST (sourceid AS INTEGER) AS sourceid
FROM raw.jocojococlient
WHERE SOURCE  = 'jocoJCMHCDemographics.PATID'
)
--Join data to Diagnoses data
SELECT 
        -- Diagnoses data
        d.dx_date, d.dx_code, d.diagnosis_description, d.diagnosis_dsm5_classification, 
        -- Other data
        d.sourcesystem,
        --IDS:
        d.patid, IDS.joid
FROM cleaned.jocojcmhcdiagnoses as d
--JOIN
FULL JOIN JCDHE_encounter AS IDS
ON d.patid = IDS.sourceid;

-------------------- Discharges data -------------------

-- JOID's
with JCDHE_encounter AS (
SELECT joid, CAST (sourceid AS INTEGER) AS sourceid
FROM raw.jocojococlient
WHERE SOURCE  = 'jocoJCMHCDemographics.PATID'
)
--Join data to Discharges data
SELECT 
        -- Diagnoses data
        d.admit_date, d.dschg_date, d.discharge_reason, d.discharge_category, d.program_description,
        -- Other data
        d.sourcesystem,
        --IDS:
        d.patid, IDS.joid
FROM cleaned.jocojcmhcdischarges as d
--JOIN
FULL JOIN JCDHE_encounter AS IDS
ON d.patid = IDS.sourceid;

-------------------- jocojcmhcdnkas -------------------

-- What is this this data? Skipping - seems irrelevant

-------------------- Income Status -------------------
-- JOID's
with JCDHE_encounter AS (
SELECT joid, CAST (sourceid AS INTEGER) AS sourceid
FROM raw.jocojococlient
WHERE SOURCE  = 'jocoJCMHCDemographics.PATID'
)
--Join data to Income data
SELECT 
        -- Income
        i.income, i.effect_date,
        -- other
        i.sourcesystem,
        --IDS:
        i.patid, IDS.joid
FROM cleaned.jocojcmhcincomestatus as i
--JOIN
FULL JOIN JCDHE_encounter AS IDS
ON i.patid = IDS.sourceid;

-------------------- Insurance Status -------------------


with JCDHE_encounter AS (
SELECT joid, CAST (sourceid AS INTEGER) AS sourceid
FROM raw.jocojococlient
WHERE SOURCE  = 'jocoJCMHCDemographics.PATID'
)
--Join data to Insurance Status
SELECT 
        -- Insurance
        i.effect_date, i.expire_date, i.payor,
        -- other
        i.sourcesystem,
        --IDS:
        i.patid, IDS.joid
FROM cleaned.jocojcmhcinsurancestatus as i
--JOIN
FULL JOIN JCDHE_encounter AS IDS
ON i.patid = IDS.sourceid;

-------------------- Mental Health Assesment Status -------------------

with JCDHE_encounter AS (
SELECT joid, CAST (sourceid AS INTEGER) AS sourceid
FROM raw.jocojococlient
WHERE SOURCE  = 'jocoJCMHCDemographics.PATID'
)
--Join data to Mental Health Assesment (Outcomes) data
SELECT 
        -- Mental Health Assesment data
        -- DLA = Daily Living Assesment (?)
        -- CAFAS = Child and Adolescent Functional Assesment Scale
        o.dla_score, o.cafas_score,o.assess_date,
        -- other
        o.sourcesystem,
        --IDS:
        o.patid, IDS.joid
FROM cleaned.jocojcmhcoutcomes as o
--JOIN
FULL JOIN JCDHE_encounter AS IDS
ON o.patid = IDS.sourceid;


-------------------- Mental Health Services Provided  -------------------

with JCDHE_encounter AS (
SELECT joid, CAST (sourceid AS INTEGER) AS sourceid
FROM raw.jocojococlient
WHERE SOURCE  = 'jocoJCMHCDemographics.PATID'
)
--Join data to Mental Health Services
SELECT 
        -- Services Data
        s.svc_date, s.svc_code, s.service_desc, s.provider_id,
        -- Other:
        s.sourcesystem,
        --IDS:
        s.patid, IDS.joid
FROM cleaned.jocojcmhcservices as s
--JOIN
FULL JOIN JCDHE_encounter AS IDS
ON s.patid = IDS.sourceid;

-------------------- Employment Data  -------------------

with JCDHE_encounter AS (
SELECT joid, CAST (sourceid AS INTEGER) AS sourceid
FROM raw.jocojococlient
WHERE SOURCE  = 'jocoJCMHCDemographics.PATID'
)
--Join data to Mental Health Services
SELECT 
        -- Vocation data
        s.status, s.status_date,
        -- Other:
        s.sourcesystem,
        --IDS:
        s.patid, IDS.joid
FROM cleaned.jocojcmhcvocationalstatus as s
--JOIN
FULL JOIN JCDHE_encounter AS IDS
ON s.patid = IDS.sourceid;



------------------
--QUERY: Retrieve information about an individual's involvement with Johnson County Justice Information Management System
-- Data source: County Level jail/inmate information - Justice Informaton Management System (JIMS)
------------------


-------------------- Get Bail Occurences  -------------------

with JCDHE_encounter AS (
SELECT joid, CAST (sourceid AS INTEGER) AS sourceid
FROM raw.jocojococlient
WHERE SOURCE  = 'jocoJIMSNameIndex.MNI_NO_0'
)
--
SELECT 
        -- Bail data
        b.arrange_date, b.amount_tendered, b.arrange_time, b.division, b.proc_type,
        -- Person data
        b.first_name_hash, b.last_name_hash, b.date_of_birth, b.race, b.sex, b.ssn_hash, b.address_hash, b.city, b.state, b.zip_code, b.joco_resident, b.home_phone_hash,
        --IDS:
        b.bail_no, b.booking_no, b.mni_no, IDS.joid
FROM cleaned.jocojimsbailhstdata as b
--JOIN
FULL JOIN JCDHE_encounter AS IDS
ON b.mni_no = IDS.sourceid;

-------------------- Bail Information -------------------
-- Join info to occurences, do analysis

WITH BAIL_NO_COUNTS AS (
SELECT COUNT(b.bail_no_0) as FREQ_BAIL_NUM from jocojimsbailhstdata b
LEFT JOIN jocojimsbailhstbailinfo bi
ON b.bail_no_0 = bi.bail_no_0
GROUP BY b.bail_no_0)
SELECT FREQ_BAIL_NUM, COUNT(FREQ_BAIL_NUM) AS COUNT
FROM BAIL_NO_COUNTS
GROUP BY FREQ_BAIL_NUM
ORDER BY FREQ_BAIL_NUM;


----- ANALYSIS: Most Bail Occurences have only 1 corresponding information row, but ~1K have 2, and a handful have 3-9


-------------------- Case Data  -------------------

with JCDHE_encounter AS (
SELECT joid, CAST (sourceid AS INTEGER) AS sourceid
FROM raw.jocojococlient
WHERE SOURCE  = 'jocoJIMSNameIndex.MNI_NO_0'
)
--
SELECT 
        -- Useful Case Data
        c.case_status, c.case_catg, c.service_type, c.bail_to_forfeit, c.set_bond_release, c.expunge_date, c.expnuge_ord_date, c.driver_license_reinstatement_date, c.other_paid, c.attorney_status, c.speedy_trial_clock, c.temp_case_flag, 
        c.no_pub, c.probno, c.weapon_forfeiture, c.weapon_release, c.suspended_date, c.wf_dt, c.wr_dt, c.cts_106, c.dv_tag, c.summons_type, c.ojw_paid_dt,
        c.speedy_days, c.cps, c.closed_date,
        -- Person data
        c.hash_first_name, c.hash_mid_name, c.hash_last_name, c.dob, c.race, c.sex, c.hash_ssn, 
        -- Address data
        c.joco_resident, c.hash_address, c.res_city, c.res_st, c.res_zip, c.hash_res_phone,
        --IDS:
        c.warrant_no, c.mni_no, c.hash_case_no,  IDS.joid
FROM cleaned.jocojimscasedata as c
--JOIN
FULL JOIN JCDHE_encounter AS IDS
ON c.mni_no = IDS.sourceid;


-------------------- Charges Information  -------------------
-- Join charges to inmate 


SELECT 
        -- Charges
        c.charge_type, c.charge_level, c.charge_chapter, c.status, c.original_jurisdriction, c.status, c.court_date, c.sentence_conditions_29, c.book_type, c.comments, c.plea, c.proj_rel_date, c.proj_rel_time, 
        --IDS:
        c.booking_no, c.charge_no, c.hash_case_number,
        --Inmate Data:
        i.mni_58
FROM cleaned.jocojimsinmatecharges as c
--JOIN TO INMATE DATA
JOIN
jocojimsinmatedata AS i
ON c.booking_no = booking_no


-- How many charges does each inmate have?
        -- Takes too long to execute?
WITH CHARGES_COUNTED AS(
SELECT COUNT( i.mni_58 ) as charge_count
FROM cleaned.jocojimsinmatecharges as c
--JOIN TO INMATE DATA
JOIN
jocojimsinmatedata AS i
ON c.booking_no = booking_no
GROUP BY i.mni_58)
SELECT charge_count, count(charge_count) as FREQ
FROM CHARGES_COUNTED
GROUP BY charge_count;


-------------------- LSI-R (Level of service inventory, revised) ------
-- Join to case no

SELECT 
        -- Summary features
        l.assessment_date, l.total_score, l.comment,
        -- ID vars
        l.lsir_no, l.hash_case_no, c.mni_no
FROM cleaned.jocojimslsirdata l
FULL JOIN cleaned.jocojimscasedata c
ON l.hash_case_no = c.hash_case_no;


------------------
--QUERY: Ambulance Encounters (doesn't work)
-- Data source: jocoMedACTIncidents
------------------


-- NOT RUN
with JCDHE_encounter AS (
SELECT joid, hash_sourceid
FROM raw.jocojococlient
WHERE SOURCE  = 'jocoMEDACTIncidents.RcdID'
)
-- Ambulance encounters 
SELECT 
        -- Summary features
        i.primaryimpression,i.secondaryimpression, i.chiefcomplaint, i.disposition,
        -- ID
        i.hash_rcdid, IDS.joid
FROM cleaned.jocomedactincidents i
--JOIN
FULL JOIN JCDHE_encounter AS IDS
ON  i.hash_rcdid = IDS.hash_sourceid
ORDER BY i.primaryimpression;


------------------
--QUERY: Police Data
-- Data source JOCOPD
------------------


-------------------- Arrests ----------------

with JCDHE_encounter AS (
SELECT joid, CAST(sourceid as integer) as sourceid
FROM raw.jocojococlient
WHERE SOURCE  = 'jocoPDArrests.NAME_ID'
)
-- Arrests data
SELECT 
        -- arrest data
        a.charge, a.arr_type, a.arstatus, a.appstate, a.addtime, a.agency, a.arrest_city,
        -- Suspect data
        a.hash_first, a.hash_middle, a.hash_last, a.hash_ssn, a.dob, a.city, a.state, a.zip, a.joco_resident,
        -- IDs
        a.name_identifier, IDS.joid, a.armainid, a.arre_id, a.case_id, a.lwmainid
FROM cleaned.jocopdarrests a
--JOIN
FULL JOIN JCDHE_encounter AS IDS
ON  a.name_identifier = IDS.sourceid

-------------------- Arrests ----------------
