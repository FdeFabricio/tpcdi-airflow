def bulk_load(conn, table, file, delimiter):
    cur = conn.cursor()
    cur.execute("""
        LOAD DATA LOCAL INFILE '{file}'
        INTO TABLE {table}
        FIELDS TERMINATED BY '{delimiter}'
        LINES TERMINATED BY \'\r\n\'
        """.format(file=file, table=table, delimiter=delimiter))
    conn.commit()


