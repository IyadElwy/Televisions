# Televisions

## This dataset was extracted, transformed and loaded using various sources. The entire ETL Process looks as follows:
![data_pipeline](https://github.com/IyadElwy/Televisions/assets/83036619/7088d477-2559-4af2-94e9-924274521d36)

## Links
### [Github ETL Process Code](https://github.com/IyadElwy/Televisions)
### [Kaggle Dataset](https://www.kaggle.com/datasets/iyadelwy/the-500mb-tv-show-dataset)

## Explanation
* First I needed to find some appropriate data-sources. For this I used Wikiquote to extract important and unique script-text from the various shows. Wikipedia was used for the extraction of generic info like summaries, etc. Metacritic was used for the extraction of user reviews (scores + opinions) and the Opensource TvMaze API was used for getting all sorts of data, ranging from titles to cast, episodes, summaries and more.
* Now the first step was to gather titles from those sources. It was important to divide the scrapers into two categories, scrapers that get the titles and scrapers that do the heavy-scraping which gets you the actual data.
* For the ETL job orchastration Apache Airflow was used which was hosted on an Azure VM instance with 4 Virtual CPUs and about 14 GBs of RAM. This was needed because of the heavy Sprak data transformations
* RDS, running PostgreSQL was used to save the titles and their corresponding urls
* S3 buckets were mostly used as Data Lakes to hold either raw data or temp data that needed to be further processed
* AWS Glue was used to do Transformations on the dataset and then output to Redshift which was our Data Warehouse in this case

## CosmosDB NoSQL Schema
It's important to note the `additionalProperties` field which makes the addition of more data to the field possible. I.e. the following fields will have alot more nested data.
```json
{
    "type": "object",
    "additionalProperties": true,
    "properties": {
        "id": {
            "type": "integer"
        },
        "title": {
            "type": "string"
        },
        "normalized_title": {
            "type": "string"
        },
        "wikipedia_url": {
            "type": "string"
        },
        "wikiquotes_url": {
            "type": "string"
        },
        "eztv_url": {
            "type": "string"
        },
        "metacritic_url": {
            "type": "string"
        },
        "wikipedia": {
            "type": "object",
            "additionalProperties": true
        },
        "wikiquotes": {
            "type": "object",
            "additionalProperties": true
        },
        "metacritic": {
            "type": "object",
            "additionalProperties": true
        },
        "tvMaze": {
            "type": "object",
            "additionalProperties": true
        }
    }
}
```

