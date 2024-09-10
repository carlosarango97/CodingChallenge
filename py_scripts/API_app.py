from pathlib import Path
from variables import DB_USERNAME, DB_SERVER, DB_PASSWORD, DB_NAME
from datetime import datetime
from utils import connect_to_database, format_json_data, validate_data, load_data_to_database
from sqlalchemy import text
import pandas as pd

def process_request(dictionary):
    path = Path('./log_records/' + datetime.now().strftime("%m-%d-%YT%H:%M:%S") + '_log.txt')
    unprocessed_data_count = 0
    processed_data_count = 0
    try:
        ### Database connection
        engine_db = connect_to_database(DB_SERVER, DB_USERNAME, DB_PASSWORD, DB_NAME)
        for item in dictionary:    
            table_name, df = format_json_data(dictionary[item], path)
            validated_df, wrong_df = validate_data(df, table_name)

            validated_count = len(validated_df.index)
            wrong_count = len(wrong_df.index)
            
            try:
                load_data_to_database(df, engine_db, table_name)
            except Exception as error:
                with path.open('a') as file:
                    message = '' +  str(error) + '\n\n'
                    file.write(message)
                wrong_count = wrong_count + validated_count
                validated_count = 0
            processed_data_count = processed_data_count + validated_count
            unprocessed_data_count = unprocessed_data_count + wrong_count
            with path.open('a') as file:
                file.write(f"The transaction result:\nSucceded records: {validated_count}\nFailed records: {wrong_count}\n\n")

    except Exception as error:
        with path.open('a') as file:
            message = '' +  str(error) + '\n\n'
            file.write(message)
        print("Something went wrong with the database connection. ", error)

    return processed_data_count, unprocessed_data_count

def report_by_quarter_get_data():
    engine = connect_to_database(DB_SERVER, DB_USERNAME, DB_PASSWORD, DB_NAME)
    with engine.connect() as connection:
        result = connection.execute(text("""
            SELECT *
            FROM (
                SELECT D.DEPARTMENT AS DEPARTMENT, J.JOB, HE.ID AS ID, 
                    CONCAT('Q',DATEPART(QUARTER,[DATETIME])) AS QUARTER_VALUE
                FROM [DIM].[HIRED_EMPLOYEES] HE
                LEFT JOIN [DIM].[JOB] J ON HE.JOB_ID = J.ID
                LEFT JOIN [DIM].[DEPARTMENT] D ON HE.DEPARTMENT_ID = D.ID
                WHERE YEAR([DATETIME]) = 2021
            ) AS SOURCE
            PIVOT (
                COUNT(ID) FOR QUARTER_VALUE IN ([Q1], [Q2], [Q3], [Q4])
            ) AS PVT
            ORDER BY 1, 2
        """))
        return_value_df = []
        for row in result:
            return_value_df.append({'department':row.DEPARTMENT, 'job':row.JOB, 'Q1':row.Q1, 'Q2':row.Q2, 'Q3':row.Q3, 'Q4':row.Q4})
        return return_value_df
    

def report_hired_employees_with_avg():
    engine = connect_to_database(DB_SERVER, DB_USERNAME, DB_PASSWORD, DB_NAME)
    with engine.connect() as connection:
        result = connection.execute(text("""
            WITH DEPARTMENTS AS (
                SELECT DEPARTMENT_ID AS IDS, D.DEPARTMENT AS DEPARTMENT, 
                    COUNT(HE.ID) AS HIRED, 
                    COUNT(HE.ID)-AVG(COUNT(HE.ID)) OVER () AS DIFF
                FROM [DIM].[HIRED_EMPLOYEES] HE
                LEFT JOIN [DIM].[DEPARTMENT] D ON HE.DEPARTMENT_ID = D.ID
                GROUP BY DEPARTMENT_ID, D.DEPARTMENT
            )
            SELECT IDS, DEPARTMENT, HIRED
            FROM DEPARTMENTS
            WHERE DIFF >= 0
        """))
        return_value_df = []
        for row in result:
            return_value_df.append({'ids':row.IDS, 'department':row.DEPARTMENT, 'hired':row.HIRED})
        return return_value_df