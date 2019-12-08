from utils.utils import bulk_load

file_path = 'data/Batch1/industry.txt'


def load(conn):
    cur = conn.cursor()    
    cur.execute("""
      DROP TABLE IF EXISTS Industry;
        CREATE TABLE Industry ( IN_ID CHAR(2) Not NULL,
                                IN_NAME CHAR(50) Not NULL,
                                IN_SC_ID CHAR(4) Not NULL
        );
    """)

    bulk_load(conn, 'Industry', file_path, '|')
