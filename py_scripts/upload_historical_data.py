import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime
from pathlib import Path

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

### Create dataframe
def get_data(filename, columns):
    return pd.read_csv('./historical_data/' + filename, names=columns)

### Definition to insert data in the tables
def upload_data(df, engine_db, table_name):
    print(f"Inserting historical data of {table_name} table")
    df.to_sql(name=table_name, con=engine_db, schema='dim', if_exists='append', index=False)

def connect_to_database(server, user, password, database):
    return create_engine(url=f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

print("Connecting to database")
try:
    ### Database settings
    db_server = 'codechallengeca.database.windows.net'
    db_user = 'carlosarango97'
    db_password = 'Ca-97080504'
    db = 'company'

    ### Database connection
    engine = connect_to_database(db_server, db_user, db_password, db)

except Exception as error:
    print("Something went wrong with the database connection. ", error)

jobs_df = get_data('jobs.csv', ['id', 'job'])
validated_jobs_df, wrong_jobs_df = validate_data(jobs_df, 'job')

departments_df = get_data('departments.csv', ['id', 'department'])
validated_departments_df, wrong_departments_df = validate_data(departments_df, 'job')

hired_employees_df = get_data('hired_employees.csv',['id','name','datetime','department_id','job_id'])
validated_hired_employees_df, wrong_hired_employees_df = validate_data(hired_employees_df, 'hired_employees')

missing_FK = pd.merge(validated_hired_employees_df, validated_jobs_df, how='left', left_on='job_id', right_on='id')[['id_x','name','datetime','department_id','job_id', 'id_y']].rename(columns={'id_x' : 'id', 'id_y' : 'jobId'})
missing_FK = pd.merge(missing_FK, validated_departments_df, how='left', left_on='department_id', right_on='id')[['id_x','name','datetime','department_id','job_id', 'jobId', 'id_y']].rename(columns={'id_x' : 'id', 'id_y' : 'departmentId'})
missing_FK = missing_FK[(missing_FK.jobId.isna() & ~missing_FK.job_id.isna()) | (missing_FK.departmentId.isna() & ~missing_FK.department_id.isna())].drop(columns=['jobId', 'departmentId'])

failed_flag = len(wrong_hired_employees_df.index) + len(wrong_departments_df.index) + len(wrong_jobs_df.index)
missing_flag = len(missing_FK.index)

if failed_flag > 0 or missing_flag > 0:
    path = Path('./log_records/' + datetime.now().strftime("%m-%d-%YT%H:%M:%S") + '_log.txt')
    if failed_flag > 0:
        with path.open('w') as file:
            file.write("The following records failed in the upload process:\n\n")
        if  (len(wrong_hired_employees_df.index) > 0):
            save_log(wrong_hired_employees_df, path, 'hired_employees')
        if  (len(wrong_departments_df.index) > 0):
            save_log(wrong_departments_df, path, 'department')
        if  (len(wrong_jobs_df.index) > 0):
            save_log(wrong_jobs_df, path, 'job')
    if missing_flag > 0:
        with path.open('a') as file:
            file.write("The following records have IDs that doesn't exist:\n\n")
        save_log(missing_FK, path, '')


### Calling definition to insert historical data
upload_data(validated_departments_df, engine, 'department')
upload_data(validated_jobs_df, engine, 'job')
upload_data(validated_hired_employees_df, engine, 'hired_employees')