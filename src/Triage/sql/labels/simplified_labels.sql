-- The combined_incarc_bjmhs CTE combines data from the prev_incarc and bjmhs CTEs from the original query.
-- It retrieves the latest booking date and bjmhs referred status for each joid within the specified date range.
WITH combined_incarc_bjmhs AS (
    -- Select the unique identifier (joid), the latest booking date, and the latest bjmhs referred status
    SELECT joid, 
           MAX(booking_date_full) AS last_book,  -- Get the latest booking date for each joid
           MAX(bjmhs_referred) AS bjmhs_sign     -- Get the latest bjmhs referred status for each joid
    FROM cleaned.jocojimsinmatedata
    -- Filter the data to only include records within the specified 5-year date range
    WHERE booking_date_full BETWEEN '{as_of_date}'::date - interval '5yr' AND '{as_of_date}'::date
    GROUP BY joid  -- Group by joid to get unique records for each joid
),

-- The simplified_pta CTE simplifies the original pta CTE by removing unnecessary joins.
-- It retrieves the joid and a binary sign indicating the mnh_flg_8 status.
simplified_pta AS (
    SELECT b.joid, 
           -- Convert the mnh_flg_8 column to a binary sign (1 for 'Y', 0 for 'N')
           -- This is used to determine if a specific flag is set for each joid
           MAX(CASE WHEN mnh_flg_8 = 'Y' THEN 1 WHEN mnh_flg_8 = 'N' THEN 0 ELSE NULL END) AS pta_sign
    FROM raw.jocojimspretrialassessdata
    -- Join with the jocojococlient table to get the joid associated with each record
    JOIN raw.jocojococlient b ON mni_no_0 = b.sourceid
    -- Filter the data to only include records within the specified 5-year date range
    WHERE create_date BETWEEN '{as_of_date}'::date - interval '5yr' AND '{as_of_date}'::date
    GROUP BY b.joid  -- Group by joid to get unique records for each joid
)

-- Main query to retrieve the final output
SELECT joid AS entity_id, 
       -- Determine the outcome based on booking and release dates
       -- The outcome is set to 1 if the booking date is within the specified range and the difference between
       -- the release date and booking date is greater than 2 weeks. Otherwise, it's set to 0.
       MAX(CASE 
              WHEN (booking_date_full BETWEEN date('{as_of_date}') AND date('{as_of_date}') + interval '{label_timespan}')
              AND (COALESCE(release_date_full, '2099-01-01 00:00:00') - booking_date_full > '2 weeks'::interval)
              THEN 1
              ELSE 0 END) AS outcome
FROM cleaned.jocojimsinmatedata 
-- Join with the combined_incarc_bjmhs CTE to get the latest booking date and bjmhs referred status for each joid
JOIN combined_incarc_bjmhs ON cleaned.jocojimsinmatedata.joid = combined_incarc_bjmhs.joid
-- Left join with the simplified_pta CTE to get the binary sign indicating the mnh_flg_8 status for each joid
LEFT JOIN simplified_pta ON cleaned.jocojimsinmatedata.joid = simplified_pta.joid
-- Filter the results to only include records where either bjmhs_sign or pta_sign is set to 1
WHERE 1 IN (bjmhs_sign, pta_sign)
GROUP BY joid;  -- Group by joid to get unique records for each joid
