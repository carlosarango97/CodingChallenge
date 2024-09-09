from pathlib import Path
from variables import DB_USERNAME, DB_SERVER, DB_PASSWORD, DB_NAME
from datetime import datetime
from utils import connect_to_database, format_json_data, validate_data, load_data_to_database

def process_request(dictionary):
    path = Path('./log_records/' + datetime.now().strftime("%m-%d-%YT%H:%M:%S") + '_log.txt')
    unprocessed_data_count = 0
    processed_data_count = 0
    
    try:
        ### Database connection
        engine = connect_to_database(DB_SERVER, DB_USERNAME, DB_PASSWORD, DB_NAME)
    except Exception as error:
        print("Something went wrong with the database connection. ", error)

    for item in dictionary:    
        table_name, df = format_json_data(dictionary[item], path)
        validated_df, wrong_df = validate_data(df, table_name)

        validated_count = len(validated_df.index)
        wrong_count = len(wrong_df.index)
        
        try:
            load_data_to_database(df, engine, table_name)
        except Exception as error:
            with path.open('a') as file:
                message = '' +  str(error) + '\n\n'
                file.write(message)
            wrong_count = wrong_count + validated_count
            validated_count = 0
        processed_data_count = processed_data_count + validated_count
        unprocessed_data_count = unprocessed_data_count + wrong_count
        with path.open('a') as file:
            file.write(f"The transaction result:\nSucceded records: {len(validated_df.index)}\nFailed records: {len(wrong_df.index)}\n\n")

    return processed_data_count, unprocessed_data_count