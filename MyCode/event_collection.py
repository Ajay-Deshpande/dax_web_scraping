from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from selenium import webdriver
import re
from time import sleep
from google.cloud import bigquery
from requests.compat import urljoin
import requests
from collections import defaultdict
from pprint import pprint
import sys
from datetime import datetime,date
import logging
import time
from pecten_utils import twitter_analytics_helpers as tah
import argparse
import os
from pathlib import Path
from pecten_utils.store_to_BQ import store_to_BQ

def get_dates(constituent_id, args):
    query = """SELECT title,place,remark,from_date,to_date  FROM `{}.{}` where constituent_id = '{}' and script_name='{}' and
    report_type='company_events'""".format(args.datasets[0], args.parameters["company_events"], constituent_id, str(Path(__file__).stem))
    result = args.storage.get_bigquery_data(query=query, iterator_flag=True)
    if result:
        return result
    return None

def get_latest_date(args,constituent_id):
    client = bigquery.Client()
    query = """SELECT max(date) as max_date FROM `{}.{}` where constituent_id = '{}'
                """.format(args.datasets[0],args.parameters["company_events"], constituent_id)
    query_job = client.query(query)
    result = query_job.result()
    if result:
        try:
            for row in result:
                max_date = row.max_date
            return max_date
        except Exception as e:
            return None

def get_storage_details(args):
    param_table = "CONSTITUENTS_FUNDAMENTAL_COLLECTION"
    parameters_list = ["company_events"]
    where = lambda x: x["source"] == 'dax'
    parameters = tah.get_parameters(args.param_connection_string, param_table, parameters_list, where)
    return parameters

def parse_table(args,table_data,constituent_details):

    args.parameters=get_storage_details(args)
    constituent_id=str(constituent_details['constituent_id'][0])
    dates = get_dates(constituent_id, args)
    if (dates):
        dates = [{"title": c[0],'place':c[1],'remark':c[2] , "from_date": str(c[3]), "to_date": str(c[3])} for c in dates]
    df1 = pd.DataFrame.from_dict(dates)
    df1 = df1.replace(np.nan, '', regex=True)
    df = table_data.rename(columns=lambda x: x.replace(' ', '_').lower())
    df['title'].replace(['/'], '', inplace=True, regex=True)
    df['title'].replace([' '], '_', inplace=True, regex=True)
    df['from_date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    df['to_date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    df = df.drop(['date'], axis=1)
    df = df.replace(np.nan, '', regex=True)
    df = df[~df[['title','place','remark','from_date','to_date']].isin(df1.to_dict('list')).all(axis=1)]
    df["constituent_name"] = constituent_details["constituent_name"][0]
    df["constituent_id"] = constituent_details["constituent_id"][0]
    df["collection_date"] = constituent_details["collection_date"][0]
    df["collection_source"] = constituent_details["collection_source"][0]
    df['last_updated_date'] = constituent_details['last_updated_date'][0]
    df["index"] = constituent_details["index"][0]
    df["time_zone"] = 'UTC'
    df["time_interval"] = 'quarterly/yearly'
    df['report_type'] = "company_events"
    df["script_name"] = str(Path(__file__).stem)
    df['report_bucket_url'] =None
    insert_data = df.to_dict(orient='records')
    primary_keys= list(insert_data[0].keys())
    store_to_BQ(args,to_insert,primary_keys,args.parameters["company_events"],'boerse-frankfurt.de')
