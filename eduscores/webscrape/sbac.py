import contextlib
import csv
import pathlib
import shutil
import sqlite3
from importlib import resources

import bs4
import requests

from eduscores import database, logs


LOGGER = logs.get_logger(__name__)

with resources.path('eduscores.data', '') as path:
    CSV_DIR = path / 'csv'
    ZIP_DIR = path / 'zip'


def get_archive_url(year, features='html.parser'):
    url = 'https://caaspp.cde.ca.gov/sb2018/ResearchFileList'
    params = {'lstTestYear': year}
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    soup = bs4.BeautifulSoup(resp.text, features=features)
    text = 'All Student Groups, comma delimited'
    anchor = next(a for a in soup('a') if text in a.text)
    url = anchor['href']
    return url


def download_archive(url, filename):
    resp = requests.get(url)
    resp.raise_for_status()

    zip_file = ZIP_DIR / f'{filename}.zip'
    with open(zip_file, 'wb') as fp:
        for chunk in resp.iter_content(chunk_size=102400):
            fp.write(chunk)

    shutil.unpack_archive(zip_file, CSV_DIR)
    

def main(years, loglevel='WARNING'):
    LOGGER.setLevel(loglevel.upper())
    LOGGER.info('started downloads')
    
    url_template = 'http://www3.cde.ca.gov/caasppresearchfiles/2018/sb/{}.zip'
    download_archive(url_template.format('subgroups'), 'subgroups')
    download_archive(url_template.format('tests'), 'tests')

    for year in years:
        try:
            url = get_archive_url(year)
            download_archive(url, f'sbac_{year}')
        except Exception as exc:
            LOGGER.exception(exc)
            raise

    LOGGER.info('finished downloads')

    database.insert.main(year, loglevel=loglevel)
