from utils.utils import bulk_load
import time

file_path = 'data/Batch1/StatusType.txt'


def load(conn):
    cur = conn.cursor()
    
    cur.execute("""
      DROP TABLE IF EXISTS StatusType;
      CREATE TABLE StatusType (
        ST_ID CHAR(4) NOT NULL,
        ST_NAME CHAR(10) NOT NULL
      );
    """)
    
    time.sleep(3)  # TODO only for testing - remove this in the future
    
    bulk_load(conn, 'StatusType', file_path, '|')
