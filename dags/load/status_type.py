import logging

from utils.utils import bulk_load

file_path = 'data/Batch1/StatusType.txt'


def load(conn):
    logging.info("Begin StatusType - Historical Load")
    cur = conn.cursor()
    
    cur.execute("""
      DROP TABLE IF EXISTS StatusType;
      CREATE TABLE StatusType (
        ST_ID CHAR(4) NOT NULL,
        ST_NAME CHAR(10) NOT NULL
      );
    """)
    
    bulk_load(conn, 'StatusType', file_path, '|')
