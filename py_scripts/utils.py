import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import json
from pathlib import Path
from variables import DB_USERNAME, DB_SERVER, DB_PASSWORD, DB_NAME

def save_log_json(value, table_name, path, is_correct):
    if value != {}:
        path = Path('./log_records/' + datetime.now().strftime("%m-%d-%YT%H:%M:%S") + '_log.txt')
        with path.open('a') as file:
            if is_correct:
                confirm_message = 'were'
            else:
                confirm_message = "couldn't be"
            file.write(datetime.now().strftime("%H:%M:%S") + '  >>>>  ' + f"The following records {confirm_message} formated for the table {table_name}:\n{json.dumps(value, indent=4)}\n\n")

def save_log(df, path, table_name):
    txt_record_value = datetime.now().strftime("%H:%M:%S") + '  ' +  table_name + '  >>>>  '
    df.insert(loc=0, column='A', value=txt_record_value)
    with path.open('a') as f:
        np.savetxt(f, df.values, fmt='%s')

def validate_data(df, table_name):
    validated_df = []
    wrong_df = []
    
    if table_name == 'job' or table_name == 'department':
        for index, row in df.iterrows():
            try:
                int(row['id'])
                if (row.iloc[1] == ''):
                    raise Exception()
                validated_df.append(index)
            except:
                wrong_df.append(index)
    elif table_name == 'hired_employees':
        for index, row in df.iterrows():
            try:
                int(row['id'])
                if (row['name'] == ''):
                    raise Exception()
                date_value = pd.to_datetime(row['datetime'], format='%Y-%m-%dT%H:%M:%SZ')
                if (np.isnan(row['job_id']) == False):
                    int(row['job_id'])
                if (np.isnan(row['department_id']) == False):
                    int(row['department_id'])
                if (pd.isnull(date_value)):
                    raise Exception()
                validated_df.append(index)
            except:
                wrong_df.append(index)
    else:
        return validated_df, wrong_df
    
    validated_df = df.loc[validated_df]
    wrong_df = df.loc[wrong_df]

    return validated_df, wrong_df

### Definition to insert data in the tables
def load_data_to_database(df, engine_db, table_name):
    print(f"Inserting historical data of {table_name} table")
    df.to_sql(name=table_name, con=engine_db, schema='dim', if_exists='append', index=False)

def connect_to_database(server, user, password, database):
    return create_engine(url=f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

def format_json_data(data, path):
    table_name, df, processed_data, unprocessed_data = format_json_table(data)
    save_log_json(processed_data, table_name, path, True)
    save_log_json(unprocessed_data, table_name, path, False)
    return table_name, df

def format_json_table(dictionary):
    table_name = dictionary['name']
    table_data = dictionary['row_data']
    processed_data = {}
    unprocessed_data = {}
    if table_name == 'job' or table_name == 'department':
        df = pd.DataFrame(columns=['id', table_name])
        for item in table_data:
            try:
                df.loc[len(df.index)] = [table_data[item]['id'], table_data[item][table_name]]
                processed_data[item] = table_data[item]
            except:
                unprocessed_data[item] = table_data[item]
    elif table_name == 'hired_employees':
        df = pd.DataFrame(columns=['id', 'name', 'datetime', 'job_id', 'department_id'])
        for item in table_data:
            try:
                df.loc[len(df.index)] = [table_data[item]['id'], table_data[item]['name'], table_data[item]['datetime'], table_data[item]['job_id'], table_data[item]['department_id']]
                processed_data[item] = table_data[item]
            except:
                unprocessed_data[item] = table_data[item]
    return table_name, df, processed_data, unprocessed_data