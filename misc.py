import sqlite3
import pandas as pd

def create_table_mappings():
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
    conn.close()
    # print(list(conn.execute('SELECT * FROM TABLE_MAPPINGS')))


"""
isin text  primary key,
constituent_name text not null,
collection_date text not null,
date text,

"""
# create table dummy(
#     l text,
#     date text default (datetime(timestamp, 'localtime'))
# )




s = storage.Storage('constituents.db')



s.run_query("""
create table dummy(
    l text,
    date text default CURRENT_DATE
)
""")
s.insert_data('dummy',[['dwe','a']],['l','date'])


"""
Date	Open	Close	High	Low	Volume €	Volume units 

create table historical_prices_and_volumes
(
    constituent_ISIN text,
    constituent_name text,
    collection_date text default CURRENT_TIMESTAMP
    date text,
    open real,
    close real,
    high real,
    low real,
    volume integer,
    volume_units integer
)

"""


def create_historical_price():
    conn = sqlite3.connect('constituents.db')
    # print(dff.columns)
    conn.execute("""
    create table historical_prices_and_volumes
    (
    constituent_ISIN text,
    constituent_name text,
    collection_date text default CURRENT_TIMESTAMP
    date text,
    open real,
    close real,
    high real,
    low real,
    volume real,
    volume_units integer
    )
    """)
    conn.close()

