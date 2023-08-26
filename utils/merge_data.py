from pyspark.sql import SparkSession
from services.aws_s3 import download_and_return_json_data_from_s3, save_csv_file_stream_to_s3
from services.aws_rds import retrieve_data, copy_from_s3_to_db

from pyspark.sql import SparkSession
from services.aws_s3 import download_and_return_json_data_from_s3
from pyspark.sql.functions import col, regexp_replace, trim
from pyspark.sql.functions import col, lower, regexp_replace
import io

import pandas as pd


spark = SparkSession.builder.appName("airflow_data_merger").getOrCreate()
initial_df_schema = ['title', 'url']


def return_dfs(bucket_name, file_names):
    dfs = []

    for file_name in file_names:
        data = download_and_return_json_data_from_s3(
            bucket_name, file_name)
        file_name_normalized = file_name.split(".json")[0]
        df = spark.createDataFrame(
            data, ['title', f'{file_name_normalized}_url'])
        dfs.append(df)

    return dfs


def merge_dfs():
    print("Retrieving s3 data")
    dfs = return_dfs('televisions-raw-titles-urls',
                     ['eztv.json', 'wikipedia.json',
                      'wikiquotes.json', 'metacritic.json'])
    print("Done retrieving s3 data")

    eztv_df = dfs[0]
    wikipedia_df = dfs[1]
    wikiquotes_df = dfs[2]
    metacritic_df = dfs[3]

    wikiquotes_df_normalized = wikiquotes_df.withColumn("normalized_title",
                                                        regexp_replace(trim(regexp_replace(lower(col("title")),
                                                                                           r"[^a-zA-Z0-9\s]", " ")), r"\s+", "_"))
    wikiquotes_df_normalized = wikiquotes_df_normalized.drop("title")

    wikipedia_df_normalized = wikipedia_df.withColumn("normalized_title",
                                                      regexp_replace(trim(regexp_replace(lower(col("title")),
                                                                                         r"[^a-zA-Z0-9\s]", " ")), r"\s+", "_"))
    wikipedia_df_normalized = wikipedia_df_normalized.drop("title")

    eztv_df_normalized = eztv_df.withColumn("normalized_title",
                                            regexp_replace(trim(regexp_replace(lower(col("title")),
                                                                               r"[^a-zA-Z0-9\s]", " ")), r"\s+", "_"))
    eztv_df_normalized = eztv_df_normalized.drop("title")

    pattern = r'\s*:\s*.*$'
    metacritic_df_no_seasons = metacritic_df.withColumn("title",
                                                        regexp_replace(col("title"), pattern, ""))
    metacritic_df_normalized = metacritic_df_no_seasons.withColumn(
        "normalized_title",
        regexp_replace(trim(regexp_replace(lower(col("title")),
                                           r"[^a-zA-Z0-9\s]", " ")), r"\s+", "_")
    )
    metacritic_df_normalized = metacritic_df_normalized.drop("title")

    result = wikipedia_df_normalized\
        .join(wikiquotes_df_normalized, 'normalized_title', 'outer')\
        .join(metacritic_df_normalized, "normalized_title", "outer") \
        .join(eztv_df_normalized, "normalized_title", "outer") \


    result = result.withColumn('title', regexp_replace(
        result.normalized_title, '_', " "))

    print("Done merging")
    return result


def merge_raw_s3_data_and_save_to_s3():
    print("Starting merging & saving of raw data to S3")

    print("Merging...")
    merged_df = merge_dfs().toPandas()
    spark.stop()

    print("Done merging.")

    new_data = []

    print("Saving to S3")
    for index, row in merged_df.iterrows():
        title = row["title"]

        retrieve_data(f"SELECT * FROM tv_shows WHERE title='{title}'")
        if len(retrieve_data) == 0:
            new_data.append(row)

    csv_buffer = io.BytesIO()
    new_data_df = pd.DataFrame(new_data)
    new_data_df.to_csv(csv_buffer, index=False, header=False, encoding='utf-8')
    save_csv_file_stream_to_s3(csv_buffer,
                               'televisions-raw-titles-urls',
                               'merged_data.csv')

    print("Done merging & saving of raw data to S3")


def copy_merged_data_from_s3_to_rds():
    print("Starting copy of merged data to postgres rds")
    copy_from_s3_to_db('televisions-raw-titles-urls', 'merged_data.csv')
    print('Done with copying the data')
