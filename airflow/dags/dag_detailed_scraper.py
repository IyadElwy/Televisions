from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from scrapers.detailed import detail_scraper_general_info_wikipedia, \
    detail_scraper_quotes_wikiquotes, detail_scraper_reviews_metacritic, detail_scraper_tvmaze
from utils.merging_with_cosmosDB import save_temp_data_needed_for_detail_scraping, delete_temp_file_needed_for_scraping


default_args = {
    'owner': 'iyadelwy',
}

with DAG('detailed_scrapers',
         default_args=default_args,
         schedule=None,
         start_date=datetime(2023, 8, 25),
         catchup=False) as dag:

    tv_maze_detailed_scraper = PythonOperator(
        python_callable=detail_scraper_tvmaze.start_scraper,
        task_id='tv_maze_detailed_scraper',
        dag=dag)
    tv_maze_detailed_scraper_merger = PythonOperator(
        python_callable=detail_scraper_tvmaze.merge_data,
        task_id='tv_maze_detailed_scraper_merger',
        dag=dag)
    tv_maze_detailed_scraper_save_to_cosmos = PythonOperator(
        python_callable=detail_scraper_tvmaze.read_merged_data_and_save_to_cosmoDB,
        task_id='tv_maze_detailed_scraper_save_to_cosmos',
        dag=dag)

    save_temp_for_other_detailed_scrapers = PythonOperator(
        python_callable=save_temp_data_needed_for_detail_scraping,
        task_id='save_temp_data_needed_for_detail_scraping',
        dag=dag)

    wikipedia_detailed_scraper = PythonOperator(
        python_callable=detail_scraper_general_info_wikipedia.start_scraper,
        task_id='wikipedia_detailed_scraper',
        dag=dag)
    wikiquotes_detailed_scraper = PythonOperator(
        python_callable=detail_scraper_quotes_wikiquotes.start_scraper,
        task_id='wikiquotes_detailed_scraper',
        dag=dag)
    metacritic_detailed_scraper = PythonOperator(
        python_callable=detail_scraper_reviews_metacritic.start_scraper,
        task_id='metacritic_detailed_scraper',
        dag=dag)

    # delete_temp_file_needed_for_detail_scraping = PythonOperator(
    #     python_callable=delete_temp_file_needed_for_scraping,
    #     task_id='delete_temp_file_needed_for_detail_scraping',
    #     dag=dag)

    tv_maze_detailed_scraper.set_downstream(
        tv_maze_detailed_scraper_merger)
    tv_maze_detailed_scraper_merger.set_downstream(
        tv_maze_detailed_scraper_save_to_cosmos)
    tv_maze_detailed_scraper_save_to_cosmos.set_downstream(
        save_temp_for_other_detailed_scrapers)

    save_temp_for_other_detailed_scrapers.set_downstream(
        wikipedia_detailed_scraper)

    wikipedia_detailed_scraper.set_downstream(wikiquotes_detailed_scraper)

    wikiquotes_detailed_scraper.set_downstream(metacritic_detailed_scraper)

    # metacritic_detailed_scraper.set_downstream(
    #     delete_temp_file_needed_for_detail_scraping)
