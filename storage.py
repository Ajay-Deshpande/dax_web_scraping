
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
        
        # self.c.close()
    # def create_table(self,table_query):
    #     # self.get_connection()
    #     self.conn.execute("""{}""".format(table_query))

        # self.c.close()


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
