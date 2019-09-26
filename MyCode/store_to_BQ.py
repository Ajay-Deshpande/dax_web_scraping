from pecten_utils.Storage import Storage
from pecten_utils.BigQueryLogsHandler import BigQueryLogsHandler
from pecten_utils.duplication_handler import DuplicationHandler
from pecten_utils.changes_handler import ChangesHandler


def store_to_BQ(args,to_insert,primary_keys,table_name,source):
    handler = DuplicationHandler(args.bucket_name)
    handler.get_files_names(str(Path(__file__).stem + "_news"))
    handler.parse_remote_files()

    changes_handler = ChangesHandler(args)
    changes_handler.add_new_cols_to_dump_table(to_insert,table_name,source)

    # PEC-279 Filter data
    not_duplicated_data, duplicate_data, invalid_data = handler.compare(to_insert,primary_keys)
    # PEC-279 Handle duplicate data
    if duplicate_data:
        handler.handle_duplicates(duplicate_data, args.duplicates_log_table,
                                  datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                  table_name, str(Path(__file__).stem ),
                                  "store_to_BQ")
        args.logger.info("duplicates logged",
                         extra={"table_name": args.duplicates_log_table, "script_type": "collection",
                                "operation": "deduplication", "dataset": args.environment,
                                "row_inserted": len(duplicate_data)})

    # PEC-279 Handle invalid data
    if invalid_data:
        handler.handle_invalid_data(invalid_data, args.invalid_log_table,
                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    table_name, str(Path(__file__).stem + "_fundamental"),
                                    "store_to_BQ", "null_primary_key")
        args.logger.info("invalid data logged",
                         extra={"table_name": args.invalid_log_table, "script_type": "collection",
                                "operation": "deduplication", "dataset": args.environment,
                                "row_inserted": len(invalid_data)})

    for dataset in args.datasets:
        try:
            result = args.storage.insert_bigquery_data(dataset, table_name, to_insert)
            if(result):
                args.logger.info("data inserted successfully", extra={"dataset": dataset, "table_name": table_name,"script_type":
                    "collection","operation": "insertion","row_inserted":len(to_insert)})
                print("Data inserted successfully")
            else:
                 args.logger.error("data not inserted", extra={"dataset": dataset, "table_name": table_name,
                                    "script_type": "collection","operation": "insertion", "row_non_inserted":len(to_insert), "criticality": 4, "data_loss":"retrievable"})
        except Exception as e:
            print(e)
            args.logger.error(e,extra={"dataset": dataset, "table_name": table_name,"script_type":"collection",
                                      "operation": "insertion","row_non_inserted":len(to_insert), "criticality": 4, "data_loss":"retrievable"})

    # PEC-279 Adding not duplicated data to handler and uploading
    handler.data_to_save += not_duplicated_data

    try:
        handler.delete_oldest_file()
        handler.save_file_to_cloud_storage(str(Path(__file__).stem + "_fundamental"),
                                           datetime.now().strftime("%Y-%m-%d_%H_%M_%S") + ".json")
    except Exception as e:
        if str(e) == "No data to save.":
            args.logger.info("Saving not_duplicated_data to GCS: {}".format(str(e)),
                              extra={"table_name": table_name,
                                     "script_type": "collection",
                                     "operation": "deduplication", "dataset": args.environment,
                                     "row_inserted": 0, "criticality": 4, "data_loss": "retrievable"})
        else:
            args.logger.error("Saving not_duplicated_data to GCS: {}".format(str(e)),
                                  extra={"table_name":table_name,
                                         "script_type": "collection",
                                         "operation": "deduplication", "dataset": args.environment,
                                         "row_inserted": 0, "criticality": 4, "data_loss":"retrievable"})
