from airflow import DAG
from airflow.operators.python import PythonOperator
from scrapers.titles import title_scraper_eztv, title_scraper_metacritic, \
    title_scraper_wikipedia, title_scraper_wikiquote

default_args = {
    'owner': 'iyadelwy',
}

with DAG('Title Scrapers', default_args=default_args, schedule_interval=None,) as dag:

    eztv_title_scraper = PythonOperator(
        python_callable=title_scraper_eztv.start_scraper,
        task_id='eztv title scraping',
        dag=dag)
    wikipedia_title_scraper = PythonOperator(
        python_callable=title_scraper_wikipedia.start_scraper,
        task_id='wikipedia title scraping',
        dag=dag)
    wikiquotes_title_scraper = PythonOperator(
        python_callable=title_scraper_wikiquote.start_scraper,
        task_id='wikiquotes title scraping',
        dag=dag)
    metacritic_title_scraper = PythonOperator(
        python_callable=title_scraper_metacritic.start_scraper,
        task_id='metacritic title scraping',
        dag=dag)

    eztv_title_scraper.set_downstream(wikipedia_title_scraper)
    wikipedia_title_scraper.set_downstream(wikiquotes_title_scraper)
    wikiquotes_title_scraper.set_downstream(metacritic_title_scraper)
