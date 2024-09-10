# CodingChallenge

This project was created to perform different features with database data. This project was developed with Azure SQL Database, Azure Data Factory, Azure Storage Account, Python and Flask

## Project structure

```
.
├── Dockerfile
├── README.md
├── dataset
│   ├── Avro1.json
│   └── AzureSqlTable1.json
├── factory
│   └── adf-codechallenge-ca.json
├── historical_data
│   ├── departments.csv
│   ├── hired_employees.csv
│   └── jobs.csv
├── linkedService
│   ├── AzureBlobStorage1.json
│   └── AzureSqlDatabase.json
├── log_records
│   ├── 09-09-2024T09:39:59_log.txt
│   ├── 09-09-2024T09:40:21_log.txt
│   ├── 09-09-2024T11:41:37_log.txt
│   ├── 09-09-2024T16:10:35_log.txt
│   ├── 09-09-2024T16:37:53_log.txt
│   └── 09-09-2024T16:38:05_log.txt
├── pipeline
│   ├── create_backup.json
│   └── restore_database.json
├── publish_config.json
├── py_scripts
│   ├── API_app.py
│   ├── API_listener.py
│   ├── static
│   │   └── styles.css
│   ├── templates
│   │   ├── api_employees_hired_by_department_job.html
│   │   └── api_hired_employees_with_avg.html
│   ├── upload_historical_data.py
│   ├── utils.py
│   └── variables.py
├── requirements.txt
└── sql_scripts
    ├── create_department_table.sql
    ├── create_hired_employees_table.sql
    ├── create_job_table.sql
    └── database_config.sql
```

## Project setup

1. Clone the repository

    ```bash
   git clone <repository_url>
   cd CodingChallenge
   ```

2. Use the API:
    2.1. With Docker:

         ```bash
         docker build --no-cache -t code-challenge-api .   
         docker run --name code-challenge-api-container -d -p 1500:1500 code-challenge-api
         ```

    2.2. Without Docker:

        ```bash
        pip install -r requirements
        ```

3. How to use the project:
    3.1. To perform the upload to the historical data:
   
        ```bash
        python py_scripts/upload_historical_data.py
       ```
   
    3.2. To use the API:
        The API will run in localhost:1500, you must call each of the end-points to perform their process.
        End-points:
   
            * GET /features/employees-hired-by-department-job: Return the report for employees hired on each quarter by area
            * GET /features/hired-employees-with-avg: Return the report for departments with a higher amount of hiring than the company average   
            * POST /bulk/tables: End-point for bulk insert

    3.3. The backup process was developed with Azure Data Factory, the pipelines are saved in ./pipeline
        Pipelines:
   
            * create_backup: This pipeline create an AVRO file for the requested table, the AVRO file will be stored in an Azure Storage Account
            * restore_database: This pipeline, using the AVRO files, restore the data of the tables: job, department and hired_employees
