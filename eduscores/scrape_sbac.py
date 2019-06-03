import logging
import shutil
from pathlib import Path

import requests
from bs4 import BeautifulSoup


data_dir = Path.cwd() / 'data'
data_dir.mkdir(exist_ok=True)

csv_dir = data_dir / 'csv'
csv_dir.mkdir(exist_ok=True)

zip_dir = data_dir / 'zip'
zip_dir.mkdir(exist_ok=True)


def get_archive_href(year):
    url = 'https://caaspp.cde.ca.gov/sb2018/ResearchFileList'
    payload = {'lstTestYear': year}
    resp = requests.get(url, params=payload)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, features='html.parser')
    text = 'All Student Groups, comma delimited'
    anchor = next(a for a in soup('a') if text in a.text)
    href = anchor['href']
    return href


def download_archive(year):
    href = get_archive_href(year)
    resp = requests.get(href)
    resp.raise_for_status()
    zip_file = zip_dir / f'sbac_{year}.zip'
    with open(zip_file, 'wb') as fp:
        for chunk in resp.iter_content(chunk_size=102400):
            fp.write(chunk)
    return


def unarchive_csv(year):
    zip_file = zip_dir / f'sbac_{year}.zip'
    shutil.unpack_archive(zip_file, csv_dir)
    return


def main():
    for year in range(2015, 2019):
        try:
            download_archive(year)
        except Exception as exc:
            logging.exception(exc)
        
        unarchive_csv(year)


if __name__ == '__main__':
    main()
