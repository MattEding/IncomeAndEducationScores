from importlib import resources

import pandas as pd

from eduscores import database


with resources.path('eduscores.data.pkl', '') as path:
    PKL_FILE = str(path / 'eduscore.pkl')


def main():
    conn = database.get_connection()

    query = '''
    SELECT * 
    FROM entity_zipcode 
    JOIN ethnicity_pct USING (cds_id) 
    JOIN gender_econ USING (cds_id)
    '''
    
    #TODO: replace '*' and '' with NULL when inserting into database
    df = pd.read_sql(query, conn).replace('*', 0)
    df.to_pickle(PKL_FILE)
