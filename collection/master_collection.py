import pandas as pd
import sqlite3
from selenium import webdriver
import sys
sys.path.append('.')
from utility.storage import Storage
from time import sleep

storage = Storage('constituents.db')
result = storage.run_query('Select * from constituents')
df = pd.DataFrame(result)
df.columns = ['constituent_ISIN','constituent_name']
url = 'https://www.boerse-frankfurt.de/equity/{}?lang=en'

driver = webdriver.Chrome('./chromedriver')
driver.maximize_window()
final_df = pd.DataFrame()

try:
    # raise Exception
    for index,row in df.iterrows():
        final_data = {}
        driver.get(url.format(row['constituent_ISIN']))
        sleep(3)
        tabs = driver.find_elements_by_xpath('//button[contains(@class,"data-menue-button")]')
        for tab in tabs:
            if tab.text == 'Charts' or tab.text == 'News':
                continue
            tab.click()
            sleep(4)
            tables = driver.find_elements_by_xpath('//table')
            table_names = []
            for table in tables:
                try:
                    table_name = table.find_element_by_xpath('./..//preceding-sibling::h2[contains(@class,"widget-table-headline")]').text
                except:
                    table_name = ''
                table_names.append(table_name)
            data = pd.read_html(driver.page_source)
            for each_df,table_name in zip(data,table_names):
                final_data[table_name] = each_df
        final_data['constituent_name'] = row['constituent_name']
        final_data['constituent_ISIN'] = row['constituent_ISIN']
        final_df = final_df.append(final_data,ignore_index = True)
        # print()
        # cols = list(map(lambda col: nocol))
        print(final_df.columns)
        final_df.to_csv('master_collection.csv')
        break
    # print('')
except Exception as e:
    print(e)
finally:
    print('Done collecting')
    df  = pd.read_csv('master_collection.csv')
    print(df.iloc[:,:2])
    print(df.dtypes)
    driver.quit()


