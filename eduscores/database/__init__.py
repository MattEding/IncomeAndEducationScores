import sqlite3
from importlib import resources


def get_connection():
    with resources.path('eduscores.data', '') as path:
        db_file = path / 'eduscore.db'
    
    return sqlite3.connect(db_file)
