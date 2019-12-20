import os
import uuid
import numpy as np
from sqlalchemy import create_engine

data_folder_path = "data/Batch1/"

inc_data_folder_path="data/Batch2/"


def bulk_load(conn, table, file, delimiter):
    cur = conn.cursor()
    cur.execute("""
        LOAD DATA INFILE '{file}'
        INTO TABLE {table}
        FIELDS TERMINATED BY '{delimiter}'
    """.format(file=file, table=table, delimiter=delimiter))
    conn.commit()


def df_bulk_load(conn, _df, table):
    tmp_file_path = data_folder_path + "tmp_" + str(uuid.uuid4())
    df = _df.fillna('\\N')
    df.to_csv(tmp_file_path, index=False, header=False, sep='|')
    bulk_load(conn, table, tmp_file_path, '|')
    os.remove(tmp_file_path)


def to_upper(value):
    if value != np.nan:
        return str(value).upper()
    return ""


def get_engine():
    return create_engine('mysql://tpcdi:pA2sw@rd@localhost/tpcdi')
