import collections
import contextlib
import sqlite3
import time

import requests
import numpy as np
from bs4 import BeautifulSoup

from eduscores import database, logs


LOGGER = logs.get_logger(__name__)


def get_soup(zipcode, features='html.parser', user_agent=''):
    url = f'https://www.unitedstateszipcodes.org/{zipcode:0>5}/'
    #: forbidden error without user-agent
    headers = {'User-Agent': user_agent}
    resp = requests.get(url, headers=headers)
    try:
        resp.raise_for_status()
    except Exception:
        if resp.status_code == requests.status_codes.codes['forbidden']:
            raise TimeoutError('exhausted daily usage')
        elif resp.status_code == requests.status_codes.codes['not_found']:
            raise ValueError(f'invalid zipcode {zipcode}')
        else:
            LOGGER.exception(f'response error ({zipcode})')
            raise
    soup = BeautifulSoup(resp.text, features=features)
    return soup


def get_data(zipcode):
    soup = get_soup(zipcode)
    #: defaultdict for named parameter sqlite3 inserts
    data = collections.defaultdict(lambda: None, {'zipcode': zipcode})
    tables = [
        table for table in soup(class_='table')
        if 'chart-legend' not in table['class']
    ]
    for table in tables:
        for row in table('tr'):
            #: clean data collected
            key = (
                row.th
                    .text
                    .lower()
                    .replace(' ', '_')
                    .split(':')[0]
            )
            val = (
                row.td
                    .text.lstrip('$')
                    .replace(',', '')
                    .split(' (')[0]
            )
            data[key] = val
    
    try:
        latitude, longitude = data['coordinates'].rstrip('ZIP').split()
    except ValueError:
        latitude = longitude = None
    finally:
        data['latitude'] = latitude
        data['longitude'] = longitude
    
    return data


def get_zipcodes(connection):
    select = '''
    SELECT DISTINCT(zipcode)
    FROM Entity
    EXCEPT
    SELECT DISTINCT(zipcode)
    FROM Zipcode;
    '''
    zipcodes = (tpl[0] for tpl in connection.execute(select))
    zipcodes = (zc for zc in zipcodes if zc.strip())
    return zipcodes


def main(seconds=0.0, loglevel="WARNING"):
    LOGGER.setLevel(loglevel.upper())

    conn = database.get_connection()
    with contextlib.closing(conn):
        zipcodes = get_zipcodes(conn)
        
        with conn:
            LOGGER.info('started zipcode inserts')
            for zipcode in zipcodes:
                try:
                    data = get_data(zipcode)
                except TimeoutError as exc:
                    LOGGER.warning(exc.args[0])
                    break
                except ValueError as exc:
                    LOGGER.info(exc.args[0])
                    continue
                except Exception as exc:
                    LOGGER.exception(exc.args[0])
                    break

                try:
                    database.insert.insert_zipcode(data, conn)
                except Exception as exc:
                    LOGGER.exception(exc.args[0])
                else:
                    LOGGER.debug(f"pending {zipcode}")

                #: avoid being locked out for daily limit
                delay = seconds + np.random.poisson() + np.random.rand()
                time.sleep(delay)

        LOGGER.info('finished zipcode inserts')
