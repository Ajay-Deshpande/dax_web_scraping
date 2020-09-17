
import sqlite3
class Storage:
    """
    Utility class with functions supporting the collectors to insert data, query data, get_latest_date in database.
    """
    def __init__(self,db_name):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
    def run_query(self,query,params=None):
        return (self.conn.execute(query))
       
    def insert_bulk(self,table_name,data):
        ## Bulk insertion.
        data.to_sql(table_name,self.conn,if_exists='append',index=False)
        print('Data inserted')
        # print(data.head())

    def get_date(self,table_name,wkn):
        """
        Get latest collection data
        """
        try:
            query = "SELECT max(collection_date) from {} where wkn = '{}'".format(table_name,wkn)
            date = list(self.conn.execute(query))[0][0]
            return date
        except:
            return None

    def insert_data(self,table_name,data,columns):
        print(data,columns)
        query = "INSERT OR IGNORE INTO {0} {1} VALUES {2}".format(table_name,tuple(columns),
        ','.join('{}'.format(tuple(each)) for each in data))
        self.conn.execute(query)
        self.conn.commit()
        
   
