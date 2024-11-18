"""Query the database"""
from pyspark.sql import SparkSession


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

spark = SparkSession.builder.appName('Databricks_Complex_Query').getOrCreate()


def query():
    result_df = spark.sql(complex_sql_query)
    print('Spark SQL executed:\n', result_df.limit(10).toPandas().to_markdown())

    if result_df.count() > 0:
        print(f'There are {result_df.count()} majors which has more employed graduates than CS and less unemployed graduates than CS:')
        result_df.show()
    else:
        print('There are no majors that have more employed graduates than CS and less unemployed graduates than CS.')
    
    return 'SQL query successfully executed.'


if __name__ == "__main__":
    query()