import contextlib
import sqlite3
from importlib import resources

from eduscores import database


def create_sbac_table(connection):
    create = '''
    CREATE TABLE IF NOT EXISTS Sbac
    (
        sbac_id                         INTEGER     PRIMARY KEY
        , county_code                   INTEGER
        , district_code                 INTEGER
        , school_code                   INTEGER
        , year                          INTEGER
        , subgroup_id                   INTEGER
        , test_type                     TEXT -- SB ELA/Literacy and Mathematics = 'B'
        , total_tested_at_entity_level  INTEGER  
        , total_tested_with_scores      INTEGER
        , grade                         INTEGER -- All grades = '13'
        , test_id                       INTEGER
        , caaspp_reported_enrollment    INTEGER
        , students_tested               INTEGER
        , mean_scale_score              REAL
        , pct_exceeded                  REAL
        , pct_met                       REAL
        , pct_nearly_met                REAL
        , pct_not_met                   REAL
        , students_with_scores_in_grade INTEGER
        , area_1_pct_above              REAL
        , area_1_pct_near               REAL
        , area_1_pct_below              REAL
        , area_2_pct_above              REAL
        , area_2_pct_near               REAL
        , area_2_pct_below              REAL
        , area_3_pct_above              REAL
        , area_3_pct_near               REAL
        , area_3_pct_below              REAL
        , area_4_pct_above              REAL
        , area_4_pct_near               REAL
        , area_4_pct_below              REAL
    );
    '''
    with connection:
        connection.execute(create)


def create_entity_table(connection):
    create = '''
    CREATE TABLE IF NOT EXISTS Entity
    (
        entity_id           INTEGER     PRIMARY KEY
        , county_code       INTEGER
        , district_code     INTEGER
        , school_code       INTEGER -- School code for a district = '0000000'
        , year              INTEGER
        , type_id           INTEGER
        , county_name       TEXT
        , district_name     TEXT
        , school_name       TEXT
        , zipcode           INTEGER
    );
    '''
    with connection:
        connection.execute(create)


def create_zipcode_table(connection):
    create = '''
    CREATE TABLE IF NOT EXISTS Zipcode
    (
        zipcode_id                  INTEGER     PRIMARY KEY
        , zipcode                   INTEGER
        , post_office_city          TEXT
        , county                    TEXT
        , timezone                  TEXT
        , area_code                 INTEGER
        , latitude                  REAL
        , longitude                 REAL
        , population                INTEGER
        , land_area                 REAL
        , water_area                REAL
        , housing_units             INTEGER
        , occupied_housing_units    INTEGER
        , median_home_value         REAL
        , median_household_income   REAL
    );
    '''
    with connection:
        connection.execute(create)


def create_subgroup_table(connection):
    create = '''
    CREATE TABLE IF NOT EXISTS Subgroup
    (
        subgroup_id     INTEGER     PRIMARY KEY
        , subgroup      TEXT
        , description   TEXT
    )
    '''
    with connection:
        connection.execute(create)


def create_test_table(connection):
    create = '''
    CREATE TABLE IF NOT EXISTS Test
    (
        test_id     INTEGER     PRIMARY KEY
        , test      TEXT
    );
    '''
    with connection:
        connection.execute(create)


def create_type_table(connection):
    create = '''
    CREATE TABLE IF NOT EXISTS Type
    (
        type_id     INTEGER     PRIMARY KEY
        , type      TEXT
    );
    '''
    with connection:
        connection.execute(create)


def create_cds_table(connection):
    create = '''
    CREATE TABLE IF NOT EXISTS Cds
    (
        cds_id          INTEGER     PRIMARY KEY
        , county_code   INTEGER
        , district_code INTEGER
        , school_code   INTEGER
    )   
    '''


def main():
    conn = database.get_connection()
    with contextlib.closing(conn):
        create_entity_table(conn)
        create_sbac_table(conn)
        create_test_table(conn)
        create_type_table(conn)
        create_subgroup_table(conn)
        create_zipcode_table(conn)
        create_cds_table(conn)
