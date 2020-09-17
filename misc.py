import sqlite3
import pandas as pd
import os

def create_historical_price():
    ## To table for historical price data
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






"""
s = storage.Storage('constituents.db')

s.run_query('''CREATE TABLE stocks
             (date text, trans text, symbol text, qty real, price real)''')

list(s.run_query('select * from stocks'))

# Insert a row of data
s.run_query("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14),('2006-01-05','BUY','RHAT',100,35.14)")

# Save (commit) the changes
s.conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
s.conn.close()


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
def clear_db():
    os.remove('constituents.db')