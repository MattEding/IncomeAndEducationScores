import itertools

from eduscores import database


def join_entity_zipcode():
    return '''
    SELECT
        county_code
        , district_code
        , school_code
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
    WHERE type NOT IN ('State', 'County', 'District')
    '''
    

def get_ethnicity_pct():
    return '''
    WITH AllStudents AS
    (
        SELECT CAST(COUNT(*) AS REAL) AS total
        FROM Sbac
        JOIN Subgroup USING (subgroup_id)
        WHERE subgroup = 'All Students'
    )

    SELECT 
        COUNT(subgroup_id) / total AS ethnicity_pct
        , subgroup_id
    FROM Sbac
    JOIN Subgroup USING (subgroup_id)
    CROSS JOIN AllStudents
    GROUP BY subgroup_id
    HAVING description = 'Ethnicity'
    ORDER BY ethnicity_pct DESC
    '''



def merge_sbac_ethnicity():
    return f'''
    SELECT *
    FROM Sbac USING (subgroup_id)
    JOIN
    (
        {get_ethnicity_pct()}
    )
    '''


def expand_sbac_gender_economics():
    query = '''
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
            total_tested_at_entity_level
            , total_tested_with_scores
            , caaspp_reported_enrollment
            , students_tested
        FROM AllGrades
    ) USING (cds_id)
    '''

    test_map = dict(ela=1, math=2)
    subgroup_map = dict(all=1, male=3, female=4, poor=31, rich=111)

    for (t, t_id), (sg, sg_id) in itertools.product(test_map.items(), subgroup_map.items()):
        subquery = f'''
        JOIN
        (
            SELECT
                pct_exceeded AS pct_exceeded_{t}_{sg}
                , pct_met AS pct_met_{t}_{sg}
                , pct_nearly_met AS pct_nearly_met_{t}_{sg}
                , pct_not_met AS pct_not_met_{t}_{sg}
            FROM AllGrades
            WHERE 
                test_id = {t_id} 
                AND subgroup_id = {sg_id}
        ) USING (cds_id)
        '''
        query += subquery
    
    query += ';'
    return query


# def create_school_view(connection):
#     create = '''
#     CREATE VIEW School AS
#     SELECT *
#     FROM Sbac
#     JOIN Type USING(type_id)
#     WHERE type_id IN ()
#     '''

# def join_data(connection):
#     query = '''
#     SELECT *
#     FROM 
#     '''


def main():
    conn = database.get_connection()
    return join_data(conn)
