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
driver.maximize_window()
final_df = pd.DataFrame()
for index,row in df.iterrows():
    final_data = {}
    driver.get(url.format(row['ISIN']))
    sleep(3)
    tabs = driver.find_elements_by_xpath('//button[contains(@class,"data-menue-button")]')
    for i in tabs:
        if i.text == 'Charts' or i.text == 'News':
            continue
        i.click()
        sleep(4)
        tables = driver.find_elements_by_xpath('//table')
        table_names = []
        for i in tables:
            try:
                table_name = i.find_element_by_xpath('./..//preceding-sibling::h2[contains(@class,"widget-table-headline")]').text
            except:
                table_name = ''
            table_names.append(table_name)
        data = pd.read_html(driver.page_source)
        for i,j in zip(data,table_names):
            final_data[j] = i
    final_data['constituent_name'] = row['constituent_name']
    final_data['constituent_ISIN'] = row['ISIN']
    final_df = final_df.append(final_data,ignore_index = True)
final_df.to_excel('master_collection.xlsx')
