# Databricks notebook source
#%run "./classes/s3_data_loader_with_mounting"
%run "./classes/s3_data_loader_with_hadoop"

# COMMAND ----------

if __name__ == "__main__":
    delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"
    aws_s3_bucket = "user-0ab336d6fcf7-bucket"

    data_loader = S3DataLoader(delta_table_path, aws_s3_bucket)

    access_key, encoded_secret_key = data_loader.load_aws_keys()
    source_url = f"s3n://{access_key}:{encoded_secret_key}@{aws_s3_bucket}"

    data_loader.mount_s3_bucket(source_url)
    data_loader.display_mounted_contents()

    file_location_pin = f"{data_loader.mount_name}/topics/0ab336d6fcf7.pin/partition=0/*.json"
    file_location_geo = f"{data_loader.mount_name}/topics/0ab336d6fcf7.geo/partition=0/*.json"
    file_location_user = f"{data_loader.mount_name}/topics/0ab336d6fcf7.user/partition=0/*.json"

    df_pin = data_loader.read_json_files(file_location_pin)
    df_geo = data_loader.read_json_files(file_location_geo)
    df_user = data_loader.read_json_files(file_location_user)

    display(df_pin)
    display(df_geo)
    display(df_user)
