import pandas as pd
from datetime import datetime
from pathlib import Path
from utils import save_log, load_data_to_database, validate_data, connect_to_database
from variables import DB_USERNAME, DB_SERVER, DB_PASSWORD, DB_NAME

engine = ''

### Create dataframe
def get_data(filename, columns):
    return pd.read_csv('./historical_data/' + filename, names=columns)

try:
    ### Database connection
    engine = connect_to_database(DB_SERVER, DB_USERNAME, DB_PASSWORD, DB_NAME)

except Exception as error:
    print("Something went wrong with the database connection. ", error)

### Execute the process if the engine was succesfully created
if engine :

    #### Get job data and validate the format of the rows
    jobs_df = get_data('jobs.csv', ['id', 'job'])
    validated_jobs_df, wrong_jobs_df = validate_data(jobs_df, 'job')

    #### Get department data and validate the format of the rows
    departments_df = get_data('departments.csv', ['id', 'department'])
    validated_departments_df, wrong_departments_df = validate_data(departments_df, 'job')

    #### Get hired employees data and validate the format of the rows
    hired_employees_df = get_data('hired_employees.csv',['id','name','datetime','department_id','job_id'])
    validated_hired_employees_df, wrong_hired_employees_df = validate_data(hired_employees_df, 'hired_employees')

    #### Check if the foreign IDs used in the hired employees data exist in the job and department tables
    missing_FK = pd.merge(validated_hired_employees_df, validated_jobs_df, how='left', left_on='job_id', right_on='id')[['id_x','name','datetime','department_id','job_id', 'id_y']].rename(columns={'id_x' : 'id', 'id_y' : 'jobId'})
    missing_FK = pd.merge(missing_FK, validated_departments_df, how='left', left_on='department_id', right_on='id')[['id_x','name','datetime','department_id','job_id', 'jobId', 'id_y']].rename(columns={'id_x' : 'id', 'id_y' : 'departmentId'})
    missing_FK = missing_FK[(missing_FK.jobId.isna() & ~missing_FK.job_id.isna()) | (missing_FK.departmentId.isna() & ~missing_FK.department_id.isna())].drop(columns=['jobId', 'departmentId'])

    failed_flag = len(wrong_hired_employees_df.index) + len(wrong_departments_df.index) + len(wrong_jobs_df.index)
    missing_flag = len(missing_FK.index)

    ##### Create log of the process
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
    load_data_to_database(validated_departments_df, engine, 'department')
    load_data_to_database(validated_jobs_df, engine, 'job')
    load_data_to_database(validated_hired_employees_df, engine, 'hired_employees')