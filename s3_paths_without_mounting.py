# Databricks notebook source
# %run "./classes/s3_data_loader_with_mounting"
%run "./classes/s3_data_loader_with_hadoop"

# COMMAND ----------

if __name__ == "__main__":
    delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"
    aws_s3_bucket = "user-0ab336d6fcf7-bucket"

    data_loader = S3DataLoader(delta_table_path, aws_s3_bucket)

    access_key, encoded_secret_key = data_loader.load_aws_keys()

    file_location_pin = f"s3n://{access_key}:{encoded_secret_key}@{aws_s3_bucket}/topics/0ab336d6fcf7.pin/partition=0/*.json"
    file_location_geo = f"s3n://{access_key}:{encoded_secret_key}@{aws_s3_bucket}/topics/0ab336d6fcf7.geo/partition=0/*.json"
    file_location_user = f"s3n://{access_key}:{encoded_secret_key}@{aws_s3_bucket}/topics/0ab336d6fcf7.user/partition=0/*.json"

    df_pin = data_loader.read_json_files(file_location_pin)
    df_geo = data_loader.read_json_files(file_location_geo)
    df_user = data_loader.read_json_files(file_location_user)

    display(df_pin)
    display(df_geo)
    display(df_user)

    #df_pin.createOrReplaceGlobalTempView("gtv_0ab336d6fcf7_pin")
    #df_geo.createOrReplaceGlobalTempView("gtv_0ab336d6fcf7_geo")
    #df_user.createOrReplaceGlobalTempView("gtv_0ab336d6fcf7_user")
