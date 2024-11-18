"""Query the database"""
from databricks import sql
from dotenv import load_dotenv
import os


# This query aims to discover majors with more employed graduates than Computer Science graduates and less unemployed graduates than Computer Science
complex_sql_query = """
SELECT db1.major, db1.major_category, db2.major, db2.major_category, 
db1.grad_employed AS cs_employed, db2.grad_employed AS other_employed,
db1.grad_unemployed AS cs_unemployed, db2.grad_unemployed AS other_unemployed
FROM default.hm246_grademployment AS db1
CROSS JOIN default.hm246_grademployment AS db2
WHERE db1.major = 'COMPUTER SCIENCE'
AND db2.grad_employed >= db1.grad_employed
AND db2.grad_unemployed < db1.grad_unemployed;
"""


def query():
    load_dotenv()
    with sql.connect(
        server_hostname=os.getenv('DATABRICKS_SERVER_HOSTNAME'),
        http_path=os.getenv('DATABRICKS_HTTP_PATH'),
        access_token=os.getenv('DATABRICKS_ACCESS_TOKEN')
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(complex_sql_query)
            result = cursor.fetchall()

            if result:
                print(f'There are {len(result)} majors which has more employed graduates than CS and less unemployed graduates than CS:')
                for row in result:
                    print(row)
                    print()
            else:
                print('There are no majors that have more employed graduates than CS and less unemployed graduates than CS.')
            
            cursor.close()
            connection.close()
    
    return 'SQL query successfully executed.'


if __name__ == "__main__":
    query()