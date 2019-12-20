def record_start(conn, batch_id):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO DImessages
        (BatchID, MessageText, MessageType)
        VALUES ({}, "start", "PCR")
    """.format(batch_id))
    conn.commit()


def record_end(conn, batch_id):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO DImessages
        (BatchID, MessageText, MessageType)
        VALUES ({}, "end", "PCR")
    """.format(batch_id))
    conn.commit()
