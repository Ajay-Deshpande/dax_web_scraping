import pandas as pd
import sqlite3
from selenium import webdriver
import sys
sys.path.append('.')
from utility.storage import Storage
from time import sleep
from cleaner import Cleaner
import datetime
from get_constituents import collect_constituent

def collect():
    storage = Storage('constituents.db')
    try:
        result = storage.run_query('Select * from constituents')
    except:
        ## If constituents under DAX isnt present fetch it by calling another script.
        collect_constituent()
        result = storage.run_query('Select * from constituents')

    df = pd.DataFrame(result)
    df.columns = ['constituent_name','wkn']
    url = 'https://www.boerse-frankfurt.de/equity/{}?lang=en'
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')
    driver = webdriver.Chrome('../chromedriver',options = options)
    final_data = {}

    try:
        for index,row in df.iterrows():
            ## To loop over each constituent page.
            final_data = {}
            driver.get(url.format(row['wkn']))
            sleep(3)
            tabs = driver.find_elements_by_xpath('//button[contains(@class,"data-menue-button")]')
            for tab in tabs:
                if tab.text in ['Charts','News','Company Details']:
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
            ## Call cleaner to cleanse and format data.
            cleaner = Cleaner(final_data)
            final_data = cleaner.clean()
            collection_date = datetime.datetime.now().strftime('%d/%m/%y')
            for table in final_data:
                ## Get the dataframe and filter out rows that are already present in database.
                latest_date = storage.get_date(table,row['wkn'])
                if not latest_date or collection_date > latest_date:
                    print('Collecting data for {} for constituent {}({})'.format(table,row['constituent_name'],row['wkn']))

                    try:
                        final_data[table]['collection_date'] = collection_date
                        final_data[table]['constituent_name'] = row['constituent_name']
                        final_data[table]['wkn'] = row['wkn']
                        ## Insert the data to database.
                        storage.insert_bulk(table,final_data[table])
                    except Exception as e:
                        print(e)
                else:
                    print('Already collected for {} for constituent {}({})'.format(table,row['constituent_name'],row['wkn']))
            break
    except Exception as e:
        print(e)
    finally:
        driver.quit()


if __name__ == '__main__':
    collect()