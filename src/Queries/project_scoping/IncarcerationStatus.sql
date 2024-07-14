-- time0: time when we start looking at someone's life data
-- time1: "today"
-- time2: time in the future, where time1-time2 is our prediction period

-- Def: IncarcerationStatus(time1):

-- Logic:
-- For every JOID, look and see if someone has ever gone to jail before time1
-- If they ever went to jail, check to see if they left jail before time1
-- If they entered jail and never left, we assume they are currently incarcerated

-- In this script, time1 = '2010-01-01 00:00:00'

-- Determine which JOIDs are currently in jail (at time1)

WITH joids_bookings AS(
SELECT  
        all_inmates.joid,
        COALESCE( current_book.currently_incarcerated, 0) AS currently_incarcerated,
        current_book.booking_date_full,
        current_book.release_date_full,
        current_book.duration_jail
FROM cleaned.jocoJIMSInmateData all_inmates
        LEFT JOIN(
                -- Get current bookings (limit to 1 booking per JOID with window function)
                SELECT
                        JOID,
                        1 AS currently_incarcerated,
                        CASE
                                WHEN release_date_full IS NULL THEN CURRENT_DATE - booking_date_full
                                ELSE release_date_full - booking_date_full 
                        END AS duration_jail,
                        booking_date_full, release_date_full,
                        ROW_NUMBER() OVER (PARTITION BY joid ORDER BY booking_date_full) AS bookings_counter       
                FROM cleaned.JocoJIMSInmateData current_book
                --Filter to only keep current incarcerations
                WHERE 
                     (booking_date_full < DATE('2010-01-01 00:00:00')
                      AND  ( 
                            release_date_full > DATE('2010-01-01 00:00:00') 
                            OR release_date_full IS NULL)
                            )
                  ) AS current_book
        ON all_inmates.joid = current_book.joid
        AND current_book.bookings_counter = 1
        
)



 -- Return JOID and Incarceration status at time t1 
SELECT *
FROM joids_bookings
ORDER BY currently_incarcerated desc, duration_jail desc


/*

-- See Summary Statistics of incarceration rates   
SELECT COUNT(JOID), currently_incarcerated
FROM joids_bookings
GROUP BY currently_incarcerated
 ;

*/

