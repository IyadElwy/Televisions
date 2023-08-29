from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from scrapers.titles import title_scraper_eztv, title_scraper_metacritic, \
    title_scraper_wikipedia, title_scraper_wikiquote
from utils.merge_data import merge_raw_s3_data_and_save_to_s3, copy_merged_data_from_s3_to_rds
from datetime import datetime

default_args = {
    'owner': 'iyadelwy',
}

with DAG('title_scrapers',
         default_args=default_args,
         schedule=None,
         start_date=datetime(2023, 8, 25),
         catchup=False) as dag:

    eztv_title_scraper = PythonOperator(
        python_callable=title_scraper_eztv.start_scraper,
        task_id='eztv_title_scraping',
        dag=dag)
    wikipedia_title_scraper = PythonOperator(
        python_callable=title_scraper_wikipedia.start_scraper,
        task_id='wikipedia_title_scraping',
        dag=dag)
    wikiquotes_title_scraper = PythonOperator(
        python_callable=title_scraper_wikiquote.start_scraper,
        task_id='wikiquotes_title_scraping',
        dag=dag)
    metacritic_title_scraper = PythonOperator(
        python_callable=title_scraper_metacritic.start_scraper,
        task_id='metacritic_title_scraping',
        dag=dag)

    merger_saver_s3 = PythonOperator(
        python_callable=merge_raw_s3_data_and_save_to_s3,
        task_id='merge_raw_s3_and_save_to_s3',
        dag=dag)

    s3_raw_to_db = PythonOperator(
        python_callable=copy_merged_data_from_s3_to_rds,
        task_id='copy_from_s3_to_db',
        dag=dag)

    trigger_task = TriggerDagRunOperator(
        task_id='trigger_detailed_scrapers_dag',
        trigger_dag_id='detailed_scrapers',
        dag=dag,
    )

    eztv_title_scraper.set_downstream(wikipedia_title_scraper)
    wikipedia_title_scraper.set_downstream(wikiquotes_title_scraper)
    wikiquotes_title_scraper.set_downstream(metacritic_title_scraper)
    metacritic_title_scraper.set_downstream(merger_saver_s3)
    merger_saver_s3.set_downstream(s3_raw_to_db)
    s3_raw_to_db.set_downstream(trigger_task)
