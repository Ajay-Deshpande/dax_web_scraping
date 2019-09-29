import pandas as pd
import sqlite3
from selenium import webdriver
from storage import Storage
from time import sleep

storage = Storage('constituents.db')
result = storage.run_query('Select * from constituents')
df = pd.DataFrame(result)
df.columns = ['ISIN','constituent_name']
url = 'https://www.boerse-frankfurt.de/equity/{}?lang=en'

driver = webdriver.Chrome('./../../chromedriver')

for index,row in df.iterrows():
	driver.get(url.format(row['ISIN']))
	sleep(3)
	data = pd.read_html(driver.page_source)
	for i in data:
		print(i.head())	
	break
