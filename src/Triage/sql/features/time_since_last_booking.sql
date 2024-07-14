SELECT
    c.joid::INT AS entity_id, 
    --cd.as_of_date,
    cd.as_of_date AS knowledge_date,
    cd.as_of_date - MAX(c.booking_date_full) AS time_since_last_booking
FROM 
    cleaned.jocojimsinmatedata c
JOIN 
    {most_recent_cohort_table_name} cd
ON 
    c.joid::INT = cd.entity_id
GROUP BY 
    c.joid, cd.as_of_date
HAVING 
    MAX(c.booking_date_full) <= cd.as_of_date
ORDER BY 
    as_of_date ASC