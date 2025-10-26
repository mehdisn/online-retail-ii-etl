/* This query idempotently inserts all dates from 2010 to 2030 
 into dim_date, if they don't already exist.
*/

INSERT INTO retail.dim_date (date)
SELECT
    toDate(arrayJoin(range(toUInt32(toDate('2010-01-01')), toUInt32(toDate('2030-01-01'))))) AS date
WHERE
    NOT EXISTS (
        SELECT 1
        FROM retail.dim_date AS dd
        WHERE dd.date = toDate(date)
    );