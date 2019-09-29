import sqlite3
import pandas as pd

df = pd.read_excel('./TABLES/Table_ Mapping.xlsx',index_col=0)
conn = sqlite3.connect('constituents.db')
# print(dff.columns)
conn.execute("""
CREATE TABLE IF NOT EXISTS TABLE_MAPPINGS(
    table_name text,
    script_name text 
)
""")

conn.executemany("INSERT OR IGNORE INTO TABLE_MAPPINGS VALUES(?,?)",df.values)
print(list(conn.execute('SELECT * FROM TABLE_MAPPINGS')))


