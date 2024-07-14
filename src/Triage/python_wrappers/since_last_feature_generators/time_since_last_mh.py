import importlib.util
import os

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Path to the helper_functions.py file
helper_functions_path = os.path.join(current_dir, '../helper_functions.py')

# Import helper functions
helper_spec = importlib.util.spec_from_file_location('helper_functions', helper_functions_path)
helper_module = importlib.util.module_from_spec(helper_spec)
helper_spec.loader.exec_module(helper_module)

query = """
WITH bjmhs AS (
  SELECT joid, MAX(bjmhs_referred) AS bjmhs_sign, MAX(booking_date_full) AS bjmhs_date
  FROM cleaned.jocojimsinmatedata
  GROUP BY joid
),
lsir AS (
  SELECT joid, MAX(case when coalesce(ans_37, ans_38, ans_39, ans_40, ans_41, ans_42, 
                        ans_43, ans_44,ans_45, ans_46, ans_47, ans_48, ans_49, ans_50) is null then NULL
                        when 1 in (ans_37, ans_38, ans_39, ans_40, ans_41, ans_42, ans_43, ans_44,ans_45, 
                        ans_46, ans_47, ans_48, ans_49, ans_50) or 0 in (ans_39, ans_40) then 1
                        else 0 
                   end) AS lsir_sign, MAX(assessment_date) AS lsir_date
  FROM cleaned.jocojimslsirdata
  GROUP BY joid
),
pta AS (
  SELECT b.joid, 
         MAX(case when mnh_flg_8 = 'Y' then 1
                  when mnh_flg_8 = 'N' then 0
                  else NULL 
             end) AS pta_sign, MAX(create_date) AS pta_date
  FROM raw.jocojimspretrialassessdata
  LEFT JOIN raw.jocojimsnameindex USING(urno)
  LEFT JOIN raw.jocojococlient b ON mni_no_0 = b.sourceid AND source = 'jocoJIMSNameIndex.MNI_NO_0'
  GROUP BY b.joid      
),
jcdhe AS (
  SELECT joid, 1 AS jcdhe_sign, MAX(encounter_date) AS jcdhe_date
  FROM cleaned.jocojcdheencounter 
  WHERE program ~ 'MENTAL HEALTH'
  GROUP BY joid
),
medact AS (
  SELECT c.joid, 1 AS medact_sign, MAX(b.incidentdate) AS medact_date
  FROM raw.jocomedactincidents a
  LEFT JOIN cleaned.jocomedactincidents b USING(id)
  LEFT JOIN raw.jocojococlient c ON c.hash_sourceid = a.hash_rcdid AND c.source = 'jocoMEDACTIncidents.RcdID'
  WHERE (primaryimpression ~ 'ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS'
         OR secondaryimpression ~ 'ANXIETY|EMOTIONAL|MENTAL|BEHAVIORAL|PSYCHIATRIC|CONSCIOUSNESS')
         AND primaryimpression !~ 'CONCUSSION'
         AND secondaryimpression !~ 'CONCUSSION'
  GROUP BY c.joid
),
jcmhc_adm AS (
  SELECT joid, 1 AS jcmhc_sign1, MAX(admit_date) AS jcmhc_adm_date
  FROM cleaned.jocojcmhcadmissions
  WHERE program_description IN ('ADULT MH PROGRAM', 'PEDIATRICS MH PROGRAM', 'EMERGENCY SERVICES', 'MOBILE CRISIS RESPONSE TEAM')
  GROUP BY joid
),
jcmhc_call AS (
  SELECT joid, 1 AS jcmhc_sign2, MAX(call_date) AS jcmhc_call_date
  FROM cleaned.jocojcmhccalldetails
  WHERE suicide_homicide_risk ILIKE '%violence%' OR suicide_homicide_risk ILIKE '%homicid%'
  GROUP BY joid
),
jail_mh_cohort AS (
  SELECT 
    joid,
    GREATEST(bjmhs_date, lsir_date, pta_date, jcdhe_date, medact_date, jcmhc_adm_date, jcmhc_call_date) AS last_mental_health_issue
  FROM bjmhs
  LEFT JOIN lsir USING(joid)
  LEFT JOIN pta USING(joid)
  LEFT JOIN jcdhe USING(joid)
  LEFT JOIN medact USING(joid)
  LEFT JOIN jcmhc_adm USING(joid)
  LEFT JOIN jcmhc_call USING(joid)
  WHERE 1 IN (bjmhs_sign, lsir_sign, pta_sign, jcdhe_sign, medact_sign, jcmhc_sign1, jcmhc_sign2)
)
SELECT 
   c.joid::INT AS entity_id, 
   cd.as_of_date AS knowledge_date,
   --last_mental_health_issue,
   cd.as_of_date - last_mental_health_issue AS days_since_last_mental_health_issue
FROM jail_mh_cohort c
JOIN 
    {most_recent_cohort_table_name} cd
ON 
    c.joid::INT = cd.entity_id
HAVING 
    MAX(c.last_mental_health_issue) <= cd.as_of_date
GROUP BY c.joid, cd.as_of_date, c.last_mental_health_issue;
""" 
helper_module.create_time_since_x_table(query, event_name="time_since_last_mh", table_name="feature_time_since_last_mh", db_engine=helper_module.create_db_engine())
