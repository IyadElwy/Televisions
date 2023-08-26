from pyspark.sql import SparkSession
from services.aws_s3 import download_and_return_json_data_from_s3
from services.aws_rds import insert_into_db

from pyspark.sql import SparkSession
from services.aws_s3 import download_and_return_json_data_from_s3
from pyspark.sql.functions import col, regexp_replace, trim
from pyspark.sql.functions import col, lower, regexp_replace


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
    return result.collect()


def merge_s3_data_and_save_to_rds():
    print("Starting merging & saving of S3 raw data to RDS")

    print("Merging...")
    rows = merge_dfs()
    print("Done merging.")

    print("Saving to rds")
    for row in rows:
        title = row["title"]
        normalized_title = row["normalized_title"]
        wikipedia_url = row["wikipedia_url"] if row["wikipedia_url"] else "Not Found"
        wikiquotes_url = row["wikiquotes_url"] if row["wikiquotes_url"] else "Not Found"
        eztv_url = row["eztv_url"] if row["eztv_url"] else "Not Found"
        metacritic_url = row["metacritic_url"] if row["metacritic_url"] else "Not Found"

        insert_into_db(title, normalized_title, wikipedia_url,
                       wikiquotes_url, metacritic_url, eztv_url)

        print(f'Saved f{row["title"]}')

    spark.stop()

    print("Done merging & saving of S3 raw data to RDS")
