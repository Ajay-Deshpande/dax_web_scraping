import sqlite3

class Storage:
    def __init__(self,db_name):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        
    def run_query(self,query):
        return list(self.conn.execute(query))

    def insert_data(self,table_name,data,columns):
        self.conn.executemany("INSERT OR IGNORE INTO {0} {1} VALUES (?,?)".format(table_name,tuple(columns)),data)
        self.conn.commit()