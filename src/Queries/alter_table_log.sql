
-- *****************************************************************************************
--   Combine booking date and time into one column "booking_date_full" 
-- *****************************************************************************************

-- ALTER TABLE cleaned.JocoJIMSInmateData
-- ADD COLUMN booking_date_full timestamp;

-- UPDATE cleaned.JocoJIMSInmateData
-- SET booking_date_full = 
--       CASE
--         WHEN booking_date IS NOT NULL AND booking_time IS NULL THEN booking_date + '00:00:01'::time
--         WHEN booking_date IS NOT NULL AND booking_time IS NOT NULL THEN booking_date + booking_time
--         WHEN booking_date IS NULL AND booking_time IS NULL THEN NULL
--  END;

-- *****************************************************************************************
--   Combine release date and time into one column "release_date_full" 
-- *****************************************************************************************

--ALTER TABLE cleaned.JocoJIMSInmateData
--ADD COLUMN release_date_full timestamp;

--UPDATE cleaned.JocoJIMSInmateData
--SET release_date_full = 
--       CASE
--         WHEN release_date IS NOT NULL AND release_time IS NULL THEN release_date + '23:59:59'::time
--         WHEN release_date IS NOT NULL AND release_time IS NOT NULL THEN release_date + release_time
--         WHEN release_date IS NULL AND release_time IS NULL THEN NULL
--  END;


-- *****************************************************************************************
--   Permanently add JOID to various tables
-- *****************************************************************************************

-- JIMSInmateData

-- ALTER TABLE cleaned.JocoJIMSInmateData
-- ADD COLUMN JOID INTEGER;

-- UPDATE cleaned.JocoJIMSInmateData AS JIMSinmate
-- SET JOID = JocoClient.JOID
-- FROM raw.jocojococlient AS JocoClient
-- WHERE CAST (JocoClient.sourceid AS INTEGER) = JIMSinmate.mni_no
-- AND JocoClient.source= 'jocoJIMSNameIndex.MNI_NO_0' ;



-- LSIR Table

/*

ALTER TABLE Cleaned.JocoJIMSCaseData 
ADD COLUMN JOID INTEGER;

UPDATE cleaned.JocoJIMSCaseData AS Cases
SET JOID = JocoClient.JOID
FROM raw.jocojococlient AS JocoClient
WHERE CAST(JocoClient.sourceid AS INTEGER) = Cases.mni_no
AND JocoClient.source= 'jocoJIMSNameIndex.MNI_NO_0' ;



 -- Admissions
 
ALTER TABLE cleaned.JocoJCMHCAdmissions
ADD COLUMN JOID INTEGER;

UPDATE cleaned.JocoJCMHCAdmissions AS cleaned_table
SET JOID = JocoClient.JOID
FROM raw.jocojococlient AS JocoClient
WHERE CAST(JocoClient.sourceid AS INTEGER) = cleaned_table.patid
AND JocoClient.source= 'jocoJCMHCDemographics.PATID';




-- Other mental health data

ALTER TABLE cleaned.JocoJCDHEEncounter
ADD COLUMN JOID INTEGER;

UPDATE cleaned.JocoJCDHEEncounter AS cleaned_table
SET JOID = JocoClient.JOID
FROM raw.jocojococlient AS JocoClient
WHERE CAST(JocoClient.sourceid AS INTEGER) = cleaned_table.patient_no
AND JocoClient.source= 'jocoJCDHEEncounter.Patient_no';




ALTER TABLE cleaned.JocoJCMHCCallDetails
ADD COLUMN JOID INTEGER;

UPDATE cleaned.JocoJCMHCCallDetails AS cleaned_table
SET JOID = JocoClient.JOID
FROM raw.jocojococlient AS JocoClient
WHERE CAST(JocoClient.sourceid AS INTEGER) = cleaned_table.patid
AND JocoClient.source= 'jocoJCMHCDemographics.PATID';


--add JOID to LSIR
ALTER TABLE cleaned.jocojimslsirdata
ADD COLUMN JOID INTEGER;

UPDATE cleaned.jocojimslsirdata AS LSIR
SET JOID = JIMScase.JOID
FROM cleaned.jocojimscasedata AS JIMScase
WHERE JIMScase.hash_case_no = LSIR.hash_case_no ;

*/

WITH joid_as_null as(
 SELECT CASE WHEN JOID IS NULL THEN 1 ELSE 0 END AS isitnull
 from cleaned.JocoJCMHCCallDetails 
 )
 SELECT 
 sum( isitnull), count(isitnull)
 from joid_as_null
 
