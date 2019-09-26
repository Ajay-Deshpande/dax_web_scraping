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
    driver=webdriver.Chrome('/home/user/Downloads/chromedriver_linux64/chromedriver',options=options)
    return driver

def getMap():
    return {'investor relation events':'fundamental_company_event_collection'}
    query = 'select * FROM COLLECTION_SCRIPT_MAPPING '
    data = args.storage.get_sql_data_text_query(args.param_connection_string, query)
    mapper={}
    for table,script in data:
        mapper[table]=script
    return mapper

def getConstituents():
    query = """select CONSTITUENT_NAME,CONSTITUENT_ID,ISIN,CONSTITUENT_SHORT_NAME
                FROM MASTER_CONSTITUENTS
                where CONSTITUENT  ='Yes' or CONSTITUENT_NAME ='DAX';
            """
    all_constituents = args.storage.get_sql_data_text_query(args.param_connection_string, query)
    all_constituents = [c for c in all_constituents]
    return all_constituents

def main(args):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = BigQueryLogsHandler(args.storage, args)
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
    args.logger = logger
    driver=getDriver()
    all_constituents=getConstituents()
    mapper=getMap()
    changes_handler = ChangesHandler(args)
    for constituent_name, constituent_id, ISIN, company_name in all_constituents:
        dt=datetime.utcnow()
        constituent_details={'constituent_name':constituent_name,
        'constituent_id':constituent_id,
        'ISIN':ISIN,
        'constituent_short_name':company_name,
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
                extra={"constituent_name":constituent_name,"constituent_id":constituent_id,
                       "table_name": table_name,"script_type": "collection",
                       "criticality": 1,"data_loss": "retrievable",
                       "operation": "collecting recent reports  data from DAX",
                       "dataset": os.environ.get("ENVIRONMENT")}
                args.logger.warning("No new report found",extra=extra)
                print("No new report found")
                continue

            remove_list=['ag','se']
            remove_list.extend(company_name.lower().split())
            new_table_name=' '.join([x for x in table_name.lower().split() if x not in remove_list])

            if(new_table_name in mapper.keys()):
                #call respective fundamental script
                script=importlib.import_module('.'+mapper[new_table_name], '.pecten_collection')
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
    args.project_name=os.environ.get('PROJECT_NAME', '')
    args.python_path = os.environ.get('PYTHON_PATH', '')
    args.google_key_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '')
    args.param_connection_string = os.environ.get('MYSQL_CONNECTION_STRING', '')
    args.environment = os.environ.get('ENVIRONMENT', '')
    args.datasets = tah.get_dataset_names(args.environment)
    args.bucket_name = os.environ.get("BUCKET_NAME", 'pecten-duplication')
    args.duplicates_log_table = os.environ.get("DUPLICATES_LOG_TABLE", 'duplicate_data_utils')
    args.invalid_log_table = os.environ.get("INVALID_LOG_TABLE", "invalid_data_utils")
    args.storage = Storage(google_key_path=args.google_key_path)
    args.download_path=r"/home/user/PycharmProj/downloads/"
    args.base_url ='https://www.boerse-frankfurt.de/equity/{}?lang=en'
    sys.path.insert(0, args.python_path)
    main(args)
