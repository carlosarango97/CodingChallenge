valor = {
    "0" :   {
                "name" : "job",
                "row_data" : {
                    0 : {"id" : 1, "job" : "valor1"},
                    1 : {"id" : 2, "job" : "valor2"},
                }
            },
    "1" :   {
                "name" : "department",
                "row_data" : {
                    0 : {"id" : 1, "department" : "valor1"},
                    1 : {"id" : 2, "department" : "valor2"},
                }
            },
    "2" :   {
                "name" : "hired_employees",
                "row_data" : {
                    0 : {"id" : 1, "name" : "valor1", "datetime" : "valor1", "job_id" : "valor1", "department_id" : "valor1"},
                    1 : {"id" : 2, "department" : "valor2"},
                    2 : {"id" : 3, "name" : "valor1", "datetime" : "valor1", "job_id" : "valor1", "department_id" : "valor1"},}
            }
}

import pandas as pd
from datetime import datetime
from pathlib import Path
import json

def save_log_json(value, path, is_correct):
    if value != {}:
        path = Path('./log_records/' + datetime.now().strftime("%m-%d-%YT%H:%M:%S") + '_log.txt')
        with path.open('a') as file:
            if is_correct:
                confirm_message = 'were'
            else:
                confirm_message = "couldn't be"
            file.write(datetime.now().strftime("%H:%M:%S") + '  >>>>  ' + f"The following records {confirm_message} proccessed:\n{json.dumps(value, indent=4)}\n\n")

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


path = Path('./log_records/' + datetime.now().strftime("%m-%d-%YT%H:%M:%S") + '_log.txt')
print(len(valor))
for valor_item in valor:
    table_name, df, processed_data, unprocessed_data = format_json_table(valor[valor_item])
    save_log_json(processed_data, path, True)
    save_log_json(unprocessed_data, path, False)
    print(table_name)
    print(df)
