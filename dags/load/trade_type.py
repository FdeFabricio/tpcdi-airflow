import logging
from utils.utils import bulk_load

file_path = 'data/Batch1/TradeType.txt'

def load(conn):
    logging.info("Begin TradeType - Historical Load")
    cur = conn.cursor()
    cur.execute("""
      DROP TABLE IF EXISTS TradeType;
        CREATE TABLE TradeType ( TT_ID CHAR(3) Not NULL,
                                    TT_NAME CHAR(12) Not NULL,
                                    TT_IS_SELL numeric(1) Not NULL,
                                    TT_IS_MRKT numeric(1) Not NULL
        );
    """)

    bulk_load(conn, 'TradeType', file_path, '|')
