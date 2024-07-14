-- Determine count of incarcerations that started between time period time1 and time2
-- Incarcerations must last at least: MinDuration in order to count as an incarceration

-- Here:
--     - time1 = 2005-01-01, time2 = 2010-01-01
--     - time1 = 2010-01-01, time2 = 2010-07-01
--     - MinDuration is 1 week no matter what 


WITH joids_incarcerations AS(
SELECT  
        all_inmates.joid,
        incarcerations.booking_date_full,
        incarcerations.release_date_full,
        incarcerations.duration_jail
FROM cleaned.jocoJIMSInmateData all_inmates
        LEFT JOIN(
                -- Get incarcerations that started before time1 with duration > MinDuration
                SELECT
                        JOID,
                        1 AS Incarceration,
                        ROW_NUMBER() OVER (PARTITION BY joid ORDER BY booking_date_full) AS incarceration_counter  
                        CASE
                                WHEN release_date_full IS NULL THEN CURRENT_DATE - booking_date_full
                                ELSE release_date_full - booking_date_full 
                        END AS duration_jail,
                        booking_date_full, release_date_full      
                FROM cleaned.JocoJIMSInmateData current_book
                --Filter to only keep current incarcerations
                WHERE 
                     booking_date_full < DATE('2005-01-01 00:00:00')
                     AND (
                                ((release_date_full - booking_date_full) > '2 weeks'::interval )
                                OR 
                                release_date_full IS NULL
                          )
                            
                  ) AS incarcerations
        ON all_inmates.joid = incarcerations.joid
)

SELECT JOID, SUM( COALESCE(Incarceration, 0)
  FROM joids_incarcerations
 GROUP BY JOID;
 

