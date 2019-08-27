WITH AllGrades AS 
(
SELECT *
FROM Sbac
JOIN Cds USING (county_code, district_code, school_code)
WHERE grade = 13 AND school_code != 0
)

SELECT *
FROM
(
SELECT DISTINCT cds_id
FROM AllGrades
)

JOIN
(
    SELECT
        cds_id
        , pct_exceeded AS pct_exceeded_ela_all
        , pct_met AS pct_met_ela_all
        , pct_nearly_met AS pct_nearly_met_ela_all
        , pct_not_met AS pct_not_met_ela_all
    FROM AllGrades
    WHERE 
        test_id = 1 
        AND subgroup_id = 1
) USING (cds_id)

JOIN
(
    SELECT
        cds_id
        , pct_exceeded AS pct_exceeded_ela_male
        , pct_met AS pct_met_ela_male
        , pct_nearly_met AS pct_nearly_met_ela_male
        , pct_not_met AS pct_not_met_ela_male
    FROM AllGrades
    WHERE 
        test_id = 1 
        AND subgroup_id = 3
) USING (cds_id)

JOIN
(
    SELECT
        cds_id
        , pct_exceeded AS pct_exceeded_ela_female
        , pct_met AS pct_met_ela_female
        , pct_nearly_met AS pct_nearly_met_ela_female
        , pct_not_met AS pct_not_met_ela_female
    FROM AllGrades
    WHERE 
        test_id = 1 
        AND subgroup_id = 4
) USING (cds_id)

JOIN
(
    SELECT
        cds_id
        , pct_exceeded AS pct_exceeded_ela_poor
        , pct_met AS pct_met_ela_poor
        , pct_nearly_met AS pct_nearly_met_ela_poor
        , pct_not_met AS pct_not_met_ela_poor
    FROM AllGrades
    WHERE 
        test_id = 1 
        AND subgroup_id = 31
) USING (cds_id)

JOIN
(
    SELECT
        cds_id
        , pct_exceeded AS pct_exceeded_ela_rich
        , pct_met AS pct_met_ela_rich
        , pct_nearly_met AS pct_nearly_met_ela_rich
        , pct_not_met AS pct_not_met_ela_rich
    FROM AllGrades
    WHERE 
        test_id = 1 
        AND subgroup_id = 111
) USING (cds_id)

JOIN
(
    SELECT
        cds_id
        , pct_exceeded AS pct_exceeded_math_all
        , pct_met AS pct_met_math_all
        , pct_nearly_met AS pct_nearly_met_math_all
        , pct_not_met AS pct_not_met_math_all
    FROM AllGrades
    WHERE 
        test_id = 2 
        AND subgroup_id = 1
) USING (cds_id)

JOIN
(
    SELECT
        cds_id
        , pct_exceeded AS pct_exceeded_math_male
        , pct_met AS pct_met_math_male
        , pct_nearly_met AS pct_nearly_met_math_male
        , pct_not_met AS pct_not_met_math_male
    FROM AllGrades
    WHERE 
        test_id = 2 
        AND subgroup_id = 3
) USING (cds_id)

JOIN
(
    SELECT
        cds_id
        , pct_exceeded AS pct_exceeded_math_female
        , pct_met AS pct_met_math_female
        , pct_nearly_met AS pct_nearly_met_math_female
        , pct_not_met AS pct_not_met_math_female
    FROM AllGrades
    WHERE 
        test_id = 2 
        AND subgroup_id = 4
) USING (cds_id)

JOIN
(
    SELECT
        cds_id
        , pct_exceeded AS pct_exceeded_math_poor
        , pct_met AS pct_met_math_poor
        , pct_nearly_met AS pct_nearly_met_math_poor
        , pct_not_met AS pct_not_met_math_poor
    FROM AllGrades
    WHERE 
        test_id = 2 
        AND subgroup_id = 31
) USING (cds_id)

JOIN
(
    SELECT
        cds_id
        , pct_exceeded AS pct_exceeded_math_rich
        , pct_met AS pct_met_math_rich
        , pct_nearly_met AS pct_nearly_met_math_rich
        , pct_not_met AS pct_not_met_math_rich
    FROM AllGrades
    WHERE 
        test_id = 2 
        AND subgroup_id = 111
) USING (cds_id)
;