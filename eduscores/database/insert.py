import contextlib
import csv
import sqlite3
from importlib import resources

from eduscores import database, logs


LOGGER = logs.get_logger(__name__)

with resources.path('eduscores.data', '') as path:
    CSV_DIR = path / 'csv'
    ZIP_DIR = path / 'zip'


def insert_entity(csv_file, connection):
    insert = '''
    INSERT INTO Entity
    (
        county_code
        , district_code
        , school_code
        , year
        , type_id
        , county_name
        , district_name
        , school_name
        , zipcode
    )
    VALUES
    (
        :county_code
        , :district_code
        , :school_code
        , :test_year
        , :type_id
        , :county_name
        , :district_name
        , :school_name
        , :zip_code
    );
    '''
    with open(csv_file, encoding='latin1') as fp:
        reader = csv.reader(fp)
        columns = [col.lower().replace(' ', '_') for col in next(reader)]

        with connection:
            for row in reader:
                data = {col: val for col, val in zip(columns, row)}

                try:
                    connection.execute(insert, data)
                except sqlite3.DatabaseError:
                    LOGGER.exception(f'error {row}')
                else:
                    LOGGER.info(f'inserted {row}')


def insert_sbac(csv_file, connection):
    insert = '''
    INSERT INTO Sbac
    (
        county_code
        , district_code
        , school_code
        , year
        , subgroup_id
        , test_type
        , total_tested_at_entity_level
        , total_tested_with_scores
        , grade
        , test_id
        , caaspp_reported_enrollment
        , students_tested
        , mean_scale_score
        , pct_exceeded
        , pct_met
        , pct_nearly_met
        , pct_not_met
        , students_with_scores_in_grade
        , area_1_pct_above
        , area_1_pct_near
        , area_1_pct_below
        , area_2_pct_above
        , area_2_pct_near
        , area_2_pct_below
        , area_3_pct_above
        , area_3_pct_near
        , area_3_pct_below
        , area_4_pct_above
        , area_4_pct_near
        , area_4_pct_below
    )
    VALUES
    (
        :county_code
        , :district_code
        , :school_code
        , :test_year
        , :subgroup_id
        , :test_type
        , :total_tested_at_entity_level
        , :total_tested_with_scores
        , :grade
        , :test_id
        , :caaspp_reported_enrollment
        , :students_tested 
        , :mean_scale_score
        , :percentage_standard_exceeded
        , :percentage_standard_met
        , :percentage_standard_nearly_met
        , :percentage_standard_not_met
        , :students_with_scores
        , :area_1_percentage_above_standard
        , :area_1_percentage_near_standard
        , :area_1_percentage_below_standard
        , :area_2_percentage_above_standard
        , :area_2_percentage_near_standard
        , :area_2_percentage_below_standard
        , :area_3_percentage_above_standard
        , :area_3_percentage_near_standard
        , :area_3_percentage_below_standard
        , :area_4_percentage_above_standard
        , :area_4_percentage_near_standard
        , :area_4_percentage_below_standard
    );
    '''
    with open(csv_file, encoding='latin1') as fp:
        reader = csv.reader(fp)
        headers = [header.lower().replace(' ', '_') for header in next(reader)]
        #: adjust for slightly different label depending on year for 'Area Near Standard'
        for idx, header in enumerate(headers):
            if ('area' in header) and ('near' in header):
                area_num_prefix = header[:7]
                headers[idx] = area_num_prefix + 'percentage_near_standard'

        with connection:
            for row in reader:
                data = {header: value for header, value in zip(headers, row)}

                try:
                    connection.execute(insert, data)
                except sqlite3.DatabaseError:
                    LOGGER.exception(f'error {row}')
                else:
                    LOGGER.info(f'inserted {row}')


def insert_subgroup(connection):
    insert = '''
    INSERT INTO Subgroup
    (subgroup_id, subgroup, description)
    VALUES
    (?, ?, ?);
    '''
    csv_file = CSV_DIR / 'Subgroups.txt'
    with open(csv_file, encoding='latin1') as fp:
        reader = csv.reader(fp)
        with connection:
            for row in reader:
                data = tuple(value.strip('" ') for value in row)[1:]
                connection.execute(insert, data)


def insert_test(connection):
    insert = '''
    INSERT INTO Test
    (test_id, test)
    VALUES
    (?, ?);
    '''
    csv_file = CSV_DIR / 'Tests.txt'
    with open(csv_file, encoding='latin1') as fp:
        reader = csv.reader(fp)
        _ = next(reader)
        with connection:
            for row in reader:
                data = tuple(value.strip('" ') for value in row)[1:]
                connection.execute(insert, data)


def insert_type(connection):
    insert = '''
    INSERT INTO Type (type_id, type)
    VALUES (?, ?);
    '''
    data = [
        (4, 'State'),
        (5, 'County'),
        (6, 'District'),
        (7, 'School'),
        (9, 'Direct Funded Charter School'),
        (10, 'Locally Funded Charter School'),
    ]
    with connection:
        connection.executemany(insert, data)


def insert_cds(connection):
    insert = '''
    INSERT INTO Cds
    (county_code, district_code, school_code)
    SELECT DISTINCT county_code, district_code, school_code
    FROM Sbac;
    '''
    with connection:
        connection.execute(insert)


def insert_zipcode(data, connection):
    insert = '''
    INSERT INTO Zipcode
    (
        zipcode
        , post_office_city
        , county
        , timezone
        , area_code
        , latitude
        , longitude
        , population
        , land_area
        , water_area
        , housing_units
        , occupied_housing_units
        , median_home_value
        , median_household_income
    )
    VALUES 
    (
        :zipcode
        , :post_office_city
        , :county
        , :timezone
        , :area_code
        , :latitude
        , :longitude
        , :population
        , :land_area
        , :water_area
        , :housing_units
        , :occupied_housing_units
        , :median_home_value
        , :median_household_income
    );
    '''
    connection.execute(insert, data)


def main(years, loglevel='WARNING'):

    conn = database.get_connection()
    with contextlib.closing(conn):
        database.table.main()

        LOGGER.setLevel(loglevel.upper())
        LOGGER.info('started inserts')

        insert_subgroup(conn)
        insert_test(conn)
        insert_type(conn)
        insert_cds(conn)

        for csv_file in CSV_DIR.iterdir():
            if not any(str(year) in csv_file.name for year in years):
                continue
            
            if 'entities' in csv_file.name:
                insert_entity(csv_file, conn)
            elif 'all_csv' in csv_file.name:
                insert_sbac(csv_file, conn)

        LOGGER.info('finished inserts')
