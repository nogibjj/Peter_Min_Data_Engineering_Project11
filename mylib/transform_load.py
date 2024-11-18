"""
Transforms and Loads data into the local SQLite3 database
"""
import csv
from databricks import sql
from dotenv import load_dotenv
import os


# load the csv file and insert into a new sqlite3 database
def load(dataset="data/grad_employment.csv"):
    payload = csv.reader(open(dataset, newline=""), delimiter=",")
    # skips the header of csv
    next(payload)

    load_dotenv()
    with sql.connect(
        server_hostname=os.getenv('DATABRICKS_SERVER_HOSTNAME'),
        http_path=os.getenv('DATABRICKS_HTTP_PATH'),
        access_token=os.getenv('DATABRICKS_ACCESS_TOKEN')
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS hm246_grademployment (
                    major_code STRING,
                    major STRING,
                    major_category STRING,
                    grad_total INTEGER,
                    grad_sample_size INTEGER,
                    grad_employed INTEGER,
                    grad_full_time_year_round INTEGER,
                    grad_unemployed INTEGER,
                    grad_unemployment_rate FLOAT,
                    grad_median INTEGER,
                    grad_P25 INTEGER,
                    grad_P75 INTEGER,
                    nongrad_total INTEGER,
                    nongrad_employed INTEGER,
                    nongrad_full_time_year_round INTEGER,
                    nongrad_unemployed INTEGER,
                    nongrad_unemployment_rate FLOAT,
                    nongrad_median INTEGER,
                    nongrad_P25 INTEGER,
                    nongrad_P75 INTEGER,
                    grad_share FLOAT,
                    grad_premium FLOAT
                )
            """
            )
            cursor.execute("""SELECT * FROM hm246_grademployment LIMIT 10;""")
            result = cursor.fetchall()
            
            if not result:
                sql_operation_str = 'INSERT INTO hm246_grademployment VALUES'
                for row in payload:
                    sql_operation_str += '\n' + str(tuple(row)) + ','
                sql_operation_str = sql_operation_str[:-1] + ';'

                cursor.execute(sql_operation_str)

            cursor.close()
            connection.close()

    return 'GradEmployment Database has been loaded or already created.'


if __name__ == "__main__":
    load()