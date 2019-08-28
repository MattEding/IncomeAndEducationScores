import itertools

from eduscores import database


def create_entity_zipcode_view(connection):
    view = '''
    CREATE VIEW entity_zipcode AS
    SELECT
        cds_id
        , zipcode
        , latitude
        , longitude
        , population
        , land_area
        , water_area
        , housing_units
        , occupied_housing_units
        , median_home_value
        , median_household_income
    FROM Entity
    JOIN Zipcode USING (zipcode)
    JOIN Type USING (type_id)
    JOIN Cds USING (county_code, district_code, school_code)
    WHERE type NOT IN ('State', 'County', 'District')
    '''
    with connection:
        connection.execute(view)
    

def create_ethnicity_pct_view(connection):
    view = '''
    CREATE VIEW ethnicity_pct AS

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
            AND caaspp_reported_enrollment IS NOT NULL
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
    '''
    with connection:
        connection.execute(view)


def create_gender_econ_view(connection):
    view = '''
    CREATE VIEW gender_econ AS

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
    '''

    test_map = dict(ela=1, math=2)
    subgroup_map = dict(all=1, male=3, female=4, poor=31, rich=111)

    for (t, t_id), (sg, sg_id) in itertools.product(test_map.items(), subgroup_map.items()):
        subquery = f'''
        JOIN
        (
            SELECT
                cds_id,
                , pct_exceeded AS pct_exceeded_{t}_{sg}
                , pct_met AS pct_met_{t}_{sg}
                , pct_nearly_met AS pct_nearly_met_{t}_{sg}
                , pct_not_met AS pct_not_met_{t}_{sg}
            FROM AllGrades
            WHERE 
                test_id = {t_id} 
                AND subgroup_id = {sg_id}
        ) USING (cds_id)
        '''
        view += subquery

    with connection:
        connection.execute(view)


def main():
    conn = database.get_connection()
    create_entity_zipcode_view(conn)
    create_ethnicity_pct_view(conn)
    create_gender_econ_view(conn)
