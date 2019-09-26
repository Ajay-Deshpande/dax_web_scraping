from google.cloud import bigquery
import argparse
import os
from pecten_utils.Storage import Storage
​
client = bigquery.Client()
​
def store_data(dataset):
	try:
        data=[
            {'table_name':'fundamentals','script_name':'fundamental_business_ratio_collection'}
            {'table_name':'master data','script_name':'fundamental_master_data_collection'}
            {'table_name':'trading parameters','script_name':'fundamental_master_data_collection'}
            {'table_name':'company reports','script_name':'fundamental_recent_report_collection'}
            {'table_name':'investor relation events','script_name':'fundamental_company_event_collection'}
            {'table_name':'dividend information','script_name':'fundamental_dividend_dax_collection'}
            {'table_name':'teechniacal key data','script_name':'fundamental_technical_figures_collection'}
            {'table_name':'historical key data','script_name':'fundamental_historical_key_data_collection'}
            ]
        args.storage.insert_bigquery_data(dataset,args.table_name,data)
	except Exception as e:
		print(e)
​
if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	args = parser.parse_args()
	args.google_key_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '')
	args.environment = os.environ.get('ENVIRONMENT', '')
	args.table_name = 'collection_script_mapping'
    args.storage = Storage(google_key_path=args.google_key_path)
	run_querry(args.environment)
