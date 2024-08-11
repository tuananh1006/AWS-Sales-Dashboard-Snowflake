import os
from snowflake.snowpark import Session
import sys
import logging

# initiate logging at info level
# logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')

# snowpark session
def get_snowpark_session() -> Session:
    connection_parameters = {
       "ACCOUNT":"....",##Fill this
        "USER": "snowpark_user",
        "PASSWORD": "Test@12$4",
        "ROLE": "SYSADMIN",
        "DATABASE":"sales_dwh",
        "SCHEMA":"source",
        "WAREHOUSE": "snowpark_etl_wh"
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()   

def traverse_directory(directory,file_extension) -> list:
    local_file_path = []
    file_name = []  # List to store CSV file paths
    partition_dir = []
    print(directory)
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(file_extension):
                file_path = os.path.join(root, file)
                file_name.append(file)
                partition_dir.append(root.replace(directory, ""))
                local_file_path.append(file_path)

    return file_name,partition_dir,local_file_path

def upload_files(file_names, partition_dirs, local_file_paths, stage_location, session):
    for index, file_element in enumerate(file_names):
        put_result = session.file.put(
            local_file_paths[index].replace("\\", "/"),
            stage_location + "/" + partition_dirs[index].replace("\\", "/"),
            auto_compress=False, overwrite=True, parallel=10
        )
        print(file_element, " => ", put_result[0].status)
def main():
    directory_path='./temp/sales/'
    stage_location = '@sales_dwh.source.my_internal_stg'
    session = get_snowpark_session()
    csv_file_name, csv_partition_dir , csv_local_file_path= traverse_directory(directory_path,'.csv')
    parquet_file_name, parquet_partition_dir , parquet_local_file_path= traverse_directory(directory_path,'.parquet')
    json_file_name, json_partition_dir , json_local_file_path= traverse_directory(directory_path,'.json')

    # Upload CSV files
    upload_files(csv_file_name, csv_partition_dir, csv_local_file_path, stage_location, session)
    # Upload JSON files
    upload_files(json_file_name, json_partition_dir, json_local_file_path, stage_location, session)

    # Upload Parquet files
    upload_files(parquet_file_name, parquet_partition_dir, parquet_local_file_path, stage_location, session)

if __name__ == '__main__':
    main()