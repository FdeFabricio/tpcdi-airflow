import numpy as np
from sqlalchemy import create_engine

data_folder_path = "data/Batch1/"


def bulk_load(conn, table, file, delimiter):
    cur = conn.cursor()
    cur.execute("""
        LOAD DATA LOCAL INFILE '{file}'
        INTO TABLE {table}
        FIELDS TERMINATED BY '{delimiter}'
        """.format(file=file, table=table, delimiter=delimiter))
    conn.commit()


def to_upper(value):
    if value != np.nan:
        return str(value).upper()
    return ""


def get_engine():
    return create_engine('mysql://root:password@localhost/tpcdi')
