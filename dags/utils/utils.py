def bulk_load(conn, table, file, delimiter):
    cur = conn.cursor()
    cur.execute("""
        LOAD DATA LOCAL INFILE '{file}'
        INTO TABLE {table}
        FIELDS TERMINATED BY '{delimiter}'
        """.format(file=file, table=table, delimiter=delimiter))
    conn.commit()
