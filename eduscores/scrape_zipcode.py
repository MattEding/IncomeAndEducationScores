import collections
import logging
import sqlite3

import pkg_resources
import requests
from bs4 import BeautifulSoup

from eduscores import database


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
    

def insert_sql(zipcode, connection):
    data = get_data(zipcode)
    insert = '''
    INSERT INTO zipcode
    (
        zip_code
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
        :zip_code
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
    return


def main():
    conn = database.get_connection()
    database.create_zipcode_table(conn)
    zipcodes = conn.execute('SELECT DISTINCT zipcode FROM entity')
    # zipcodes = [95408, 95488]  # first zipcode lacks most info tables
    for zipcode in zipcodes:
        try:
            insert_sql(zipcode, conn)
        except Exception as exc:
            logging.exception(exc)
    conn.commit()
    conn.close()


if __name__ == '__main__':
    # main()
    import pathlib
    dne = pathlib.Path(pkg_resources.resource_filename(__package__, 'dne'))
    print(dne)

