import requests
from dotenv import load_dotenv
import os
import json
import base64
from pyspark.sql import SparkSession
from query import query


# Load environment variables
load_dotenv('../.env')
server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/peter_min_data_engineering_project11"
auth_headers = {'Authorization': f'Bearer {access_token}'}
url = f"https://{server_hostname}/api/2.0"

LOG_FILE = "Output_WRData.md"


# Helper Functions
def log_output(operation, output, query=None):
    """Logs output to a markdown file."""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query: 
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")


def perform_request(path, method="POST", data=None):
    """Performs an HTTP request to the Databricks API."""
    session = requests.Session()
    response = session.request(
        method=method,
        url=f"{url}{path}",
        headers=auth_headers,
        data=json.dumps(data) if data else None,
        verify=True
    )
    return response.json()


def upload_file_from_url(url, dbfs_path, overwrite):
    """Uploads a file from a URL to DBFS."""
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        # Create file handle
        handle = perform_request("/dbfs/create", 
                                 data={"path": dbfs_path, 
                                       "overwrite": overwrite})["handle"]
        print(f"Uploading file: {dbfs_path}")
        # Add file content in chunks
        for i in range(0, len(content), 2**20):
            perform_request(
                "/dbfs/add-block",
                data={"handle": handle, 
                      "data": base64.standard_b64encode(content[i:i+2**20]).decode()}
            )
        # Close the handle
        perform_request("/dbfs/close", data={"handle": handle})
        print(f"File {dbfs_path} uploaded successfully.")
    else:
        print("Failed to download")


# ETL Functions
def extract(
    source_url=
    "https://github.com/fivethirtyeight/data/blob/master/college-majors/grad-students.csv?raw=true",
    target_path=FILESTORE_PATH+"/grad-students.csv",
    directory=FILESTORE_PATH,
    overwrite=True
):
    """Extracts and uploads data."""
    # Create directory on DBFS
    perform_request("/dbfs/mkdirs", data={"path": directory})
    # Upload file to DBFS
    upload_file_from_url(source_url, target_path, overwrite)
    return target_path


def transform_and_load(dataset=FILESTORE_PATH+"/grad-students.csv"):
    """Transforms and loads data into Delta Lake."""
    spark = SparkSession.builder.appName("Transform and Load WRRankings").getOrCreate()

    # Read the CSV file with the original schema (infer column types)
    df = spark.read.csv(dataset, header=True, inferSchema=True)

    # Save the transformed data as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("grad_employment_delta")

    # Log the output of the transformation
    num_rows = df.count()
    print(f"Number of rows in the transformed dataset: {num_rows}")
    log_output("10 rows preview of transformed dataset:\n", df.limit(10).toPandas().to_markdown())

    return "Transformation and loading completed successfully."


# Run the ETL pipeline
if __name__ == "__main__":
    extract()  # Step 1: Extract data
    transform_and_load()  # Step 2: Transform and load the data
    query()  # Step 3: Query the transformed data