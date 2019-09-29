from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import pandas as pd 
from time import sleep
import sqlite3


class Storage:
    def __init__(self,db_name):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        # self.c = self.conn.cursor()
    def run_query(self,query,params=None):
        return (self.conn.execute(query))
        

    def insert_data(self,table_name,data,columns):
        
        #ignore if data is already present
        self.conn.executemany("INSERT OR IGNORE INTO {0} {1} VALUES (?,?)".format(table_name,tuple(columns)),data)
        self.conn.commit()
        
        # self.c.close()
    # def create_table(self,table_query):
    #     # self.get_connection()
    #     self.conn.execute("""{}""".format(table_query))

        # self.c.close()





storage = Storage('constituents.db')


driver = webdriver.Chrome('./chromedriver')

try:
    driver.maximize_window()
    driver.get('https://www.boerse-frankfurt.de/?lang=en')
    driver.find_element_by_xpath('//button[contains(text(),"Accept")]').click()
    search_bar = driver.find_element_by_xpath('//input')
    search_bar.clear()
    search_bar.send_keys("DAX")
    sleep(3)
    search_bar.send_keys(Keys.ENTER)
    print(driver.current_url)
    dax_page = driver.find_element_by_xpath('//a[contains(text(),"DAX")]').get_attribute('href')
    driver.get(dax_page)
    sleep(3)
    driver.find_element_by_xpath('//button[contains(text(),"Constituents")]').click()
    sleep(3)
    driver.find_element_by_xpath('//button[contains(@class,"page-bar-type-button") and contains(text(),"100")]').click()
    sleep(3)
    lst = driver.find_elements_by_xpath('//table/tbody/tr/td/div/a')
    print(len(lst))
    df = pd.DataFrame()
    for i in lst:
        df = df.append({'constituent_name' : i.text, 'ISIN' : i.get_attribute('href').split('/')[-1]},ignore_index=True)
    
    

    #create table
    # storage.run_query("""create table if not exists constituents(
    #     ISIN  text not null primary key,
    #     constituents_name text
    # )""")
    #table_name,data,list of column names(string)
    storage.insert_data('constituents',df.values,['ISIN','constituents_name'])

    print(list(storage.run_query('select * from constituents')))
    driver.quit()
except Exception as e:
    print(e)
    driver.quit()
