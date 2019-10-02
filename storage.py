
import sqlite3
class Storage:
    def __init__(self,db_name):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        # self.c = self.conn.cursor()
    def run_query(self,query,params=None):
        return (self.conn.execute(query))
        

    def insert_data(self,table_name,data,columns):
        print(data,columns)
        query = "INSERT OR IGNORE INTO {0} {1} VALUES {2}".format(table_name,tuple(columns),
        ','.join('{}'.format(tuple(each)) for each in data))
        # print(query)
        #ignore if data is already present
        self.conn.execute(query)
        self.conn.commit()
        
   
