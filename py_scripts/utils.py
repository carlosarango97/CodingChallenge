import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd

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