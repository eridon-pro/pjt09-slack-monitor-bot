import sqlite3
from datetime import datetime

conn = sqlite3.connect("scores.db")
cur = conn.cursor()

cur.execute("SELECT id, ts FROM events WHERE ts IS NOT NULL")
for row in cur.fetchall():
    eid, ts_str = row
    try:
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        ts_epoch = dt.timestamp()
        cur.execute("UPDATE events SET ts_epoch = ? WHERE id = ?", (ts_epoch, eid))
    except Exception as e:
        print("SKIP", eid, ts_str, e)

conn.commit()
conn.close()
print("done.")
