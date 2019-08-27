
WITH AllStudents AS
(
    SELECT 
        cds_id
        , CAST(caaspp_reported_enrollment AS REAL) AS total
    FROM 
    (
        SELECT *
        FROM Sbac
        WHERE grade = 13 AND school_code != 0
        AND caaspp_reported_enrollment != '*'
    )
    JOIN Subgroup USING (subgroup_id)
    JOIN Cds USING (county_code, district_code, school_code)
    GROUP BY cds_id
    HAVING 
        subgroup = 'All Students'
)
, AllGrades AS 
(
    SELECT *
    FROM Sbac
    JOIN Cds USING (county_code, district_code, school_code)
    WHERE grade = 13 AND school_code != 0
)

SELECT 
    cds_id
    , IFNULL(MAX(CASE subgroup_id WHEN 74 THEN caaspp_reported_enrollment / total END), 0.0) as pct_black
    , IFNULL(MAX(CASE subgroup_id WHEN 75 THEN caaspp_reported_enrollment / total END), 0.0) as pct_native
    , IFNULL(MAX(CASE subgroup_id WHEN 76 THEN caaspp_reported_enrollment / total END), 0.0) as pct_asian
    , IFNULL(MAX(CASE subgroup_id WHEN 77 THEN caaspp_reported_enrollment / total END), 0.0) as pct_filipino
    , IFNULL(MAX(CASE subgroup_id WHEN 78 THEN caaspp_reported_enrollment / total END), 0.0) as pct_latino
    , IFNULL(MAX(CASE subgroup_id WHEN 79 THEN caaspp_reported_enrollment / total END), 0.0) as pct_islander
    , IFNULL(MAX(CASE subgroup_id WHEN 80 THEN caaspp_reported_enrollment / total END), 0.0) as pct_white
FROM AllGrades
JOIN AllStudents USING (cds_id)
JOIN Subgroup USING (subgroup_id)
GROUP BY cds_id
HAVING description = 'Ethnicity'
;