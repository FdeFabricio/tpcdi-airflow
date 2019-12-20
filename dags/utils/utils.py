import numpy as np
from sqlalchemy import create_engine

data_folder_path = "data/Batch1/"
inc_data_folder_path = "data/Batch2/"


def to_upper(value):
    if value != np.nan:
        return str(value).upper()
    return ""


def get_engine():
    return create_engine('mysql://tpcdi:pA2sw@rd@localhost/tpcdi')
