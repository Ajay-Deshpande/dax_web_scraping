import pandas as pd
import sqlite3
from selenium import webdriver
import sys
sys.path.append('./../utility')
from storage import Storage
from time import sleep
from cleaner import Cleaner

def collect():
    storage = Storage('./../constituents.db')
    result = storage.run_query('Select * from constituents')
    df = pd.DataFrame(result)
    df.columns = ['constituent_ISIN','constituent_name']
    url = 'https://www.boerse-frankfurt.de/equity/{}?lang=en'
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')
    driver = webdriver.Chrome('./../../../chromedriver',options = options)
    final_df = pd.DataFrame()

    try:
        # raise Exception
        for index,row in df.iterrows():
            final_data = {}
            driver.get(url.format(row['constituent_ISIN']))
            sleep(3)
            tabs = driver.find_elements_by_xpath('//button[contains(@class,"data-menue-button")]')
            for tab in tabs:
                if tab.text in ['Charts','News']:
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
                    if table_name.find(row['constituent_name']) != -1:
                        table_name = table_name[ :table_name.find(row['constituent_name'])].strip()
                    table_names.append(table_name)
                data = pd.read_html(driver.page_source)
                for each_df,table_name in zip(data,table_names):
                    if not table_name:
                        continue
                    final_data[table_name] = each_df
            cleaner = Cleaner(final_data)
            final_data = cleaner.clean()
            final_data['constituent_name'] = row['constituent_name']
            final_data['constituent_ISIN'] = row['constituent_ISIN']
            final_df = final_df.append(final_data,ignore_index = True)
            break
    except Exception as e:
        print(e)
    finally:
        print('Done collecting and cleaning')
        driver.quit()
