import collections
import logging
import sqlite3
from pathlib import Path

import requests
from bs4 import BeautifulSoup


def get_soup(zipcode):
    url = f'https://www.unitedstateszipcodes.org/{zipcode}/'
    headers = {'User-Agent': ''}  #: 403 error without user-agent
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, features='html.parser')
    return soup


def get_data(zipcode):
    soup = get_soup(zipcode)
    data = collections.defaultdict(lambda: None, {'zipcode': zipcode})  #: defaultdict for named parameter sqlite3 inserts
    tables = [table for table in soup(class_='table') if 'chart-legend' not in table['class']]
    for table in tables:
        for tr in table('tr'):
            key = tr.th.text.lower().replace(' ', '_').split(':')[0]
            val = tr.td.text.lstrip('$').replace(',', '').split(' (')[0]
            data[key] = val
    
    try:
        latitude, longitude = data['coordinates'].rstrip('ZIP').split()
    except ValueError:
        latitude = longitude = None
    data['latitude'] = latitude
    data['longitude'] = longitude
    return data


def get_connection():
    db_file = Path().cwd() / 'data' / 'eduscore.db'
    conn = sqlite3.connect(db_file)
    return conn


def create_table(connection):
    statement = '''
    CREATE TABLE IF NOT EXISTS zipcode
    (
        zipcode TEXT PRIMARY KEY  -- zipcodes can start with 0 so not INTEGER
        , post_office_city TEXT
        , county TEXT
        , timezone TEXT
        , area_code INTEGER
        , latitude REAL
        , longitude REAL
        , population INTEGER
        , land_area REAL
        , water_area REAL
        , housing_units INTEGER
        , occupied_housing_units INTEGER
        , median_home_value REAL
        , median_household_income REAL
        
    );
    '''
    connection.execute(statement)
    return
    

def insert_sql(zipcode, connection):
    data = get_data(zipcode)
    statement = '''
    INSERT INTO zipcode
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
    connection.execute(statement, data)
    return


def main():
    conn = get_connection()
    create_table(conn)
    zipcodes = conn.execute('SELECT DISTINCT zipcode FROM entity')
    for zipcode in zipcodes:
        try:
            insert_sql(zipcode, conn)
        except Exception as exc:
            logging.exception(exc)
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
