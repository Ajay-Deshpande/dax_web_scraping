from pkg_resources import resource_filename
from selenium import webdriver
from importlib import import_module
import re
from time import sleep
from selenium import webdriver
from datetime import datetime
import os
import logging
import sys
import argparse
from pecten_utils.Storage import Storage
from pecten_utils.BigQueryLogsHandler import BigQueryLogsHandler
from pecten_utils.changes_handler import ChangesHandler
from sqlalchemy import *
import pandas as pd
from pecten_utils.get_table import click_elements_and_get_tables
from pecten_utils import twitter_analytics_helpers as tah

def get_tabs(driver):
    return driver.find_element_by_xpath('/html/body/app-root/app-wrapper/div/div[2]/app-equity/app-data-menue/div/div/div/drag-scroll/div/div').find_elements_by_tag_name('button')

def getDriver():
    #This function returns the web driver that calls the web webpage
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-extensions')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_experimental_option("prefs", {
        "download.default_directory": args.download_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True })
    driver=webdriver.Chrome(args.driver_path,options=options)
    return driver

def getMap():
#    return {'investor relation events':'fundamental_company_event_collection'}
    df = pd.read_excel(args.table_path+'Table_ Mapping.xlsx',sheetname='Sheet1')
    mapper = df.set_index('Table_Name').to_dict()['script_name']
    print(mapper)
    return mapper

def getConstituents():
    all_constituents = pd.read_excel(args.table_path+'Constituent_ISIN.xlsx',sheetname='Sheet1').values.tolist()
    return all_constituents


def main(args):
    driver=getDriver()
    all_constituents=getConstituents()
    mapper=getMap()
    changes_handler = ChangesHandler(args)
    for ISIN, constituent_name in all_constituents:
        dt=datetime.utcnow()
        constituent_details={'constituent_name':constituent_name,
        'ISIN':ISIN,
        'collection_source':'boerse-frankfurt.de',
        'collection_date': dt.date().strftime('%Y-%m-%d'),
        'last_updated_date': dt.strftime('%Y-%m-%d %H:%M:%S'),
        'index':'DAX'}
        constituent_details = pd.DataFrame().append(constituent_details, ignore_index=True)

        url = args.base_url.format(ISIN)
        driver.get(url)
        sleep(8)
        tabs=get_tabs(driver)
        all_tables=click_elements_and_get_tables(args,driver,tabs)
        print('Details of {} collected'.format(company_name))

        for table_name in all_tables.keys():
            table_data=all_tables[table_name]
            if (table_data.shape[0] == 0):
                print("No data present in",table_name)
                continue

            remove_list=['ag','se']
            remove_list.extend(company_name.lower().split())
            new_table_name=' '.join([x for x in table_name.lower().split() if x not in remove_list])

            if(new_table_name in mapper.keys()):
                #call respective fundamental script
                script=importlib.import_module('.'+mapper[new_table_name])
                script.parse_table(args,table_data,constituent_details)
                print('{} is parsed'.format(new_table_name))

            else:
                #store to dump
                print('Script for {} not present'.format(new_table_name))
                single_row_tables=['price information', 'fundamentals',  'master data', 'trading parameters frankfurt', 'contact', 'corporate information', 'technical key data']
                if(new_table_name in single_row_tables):
                    table_data=table_data.set_index(['Column 0']).T
                for i in constituent_details.columns:
                    table_data[i] = constituent_details[i][0]
                changes_handler.add_new_cols_to_dump_table(table_data,new_table_name,'boerse-frankfurt.de')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    args.python_path = os.environ.get('PYTHON_PATH', '')
    args.table_path='./TABLES/'
    args.driver_path='/home/user/Downloads/chromedriver_linux64/chromedriver'
    args.download_path=r"/home/user/PycharmProj/downloads/"
    args.base_url ='https://www.boerse-frankfurt.de/equity/{}?lang=en'
    sys.path.insert(0, args.python_path)
    main(args)
