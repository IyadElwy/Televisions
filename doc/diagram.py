from diagrams import Diagram
from diagrams.aws.database import RDS
from diagrams.aws.storage import S3
from diagrams import Diagram, Cluster, Edge
from diagrams import Diagram, Cluster
from diagrams.onprem.workflow import Airflow
from diagrams.custom import Custom
from diagrams.aws.analytics import GlueCrawlers,\
    GlueDataCatalog, Redshift, Glue, Athena, Quicksight
from diagrams.onprem.analytics import Tableau
from diagrams.aws.compute import Lambda
from diagrams.saas.chat import Slack
from diagrams.azure.database import CosmosDb

with Diagram('Televisions Data Pipeline', show=False, filename='doc/data_pipeline'):

    data_sources = []
    with Cluster('  Data Sources'):

        data_sources = [Custom('Wikiquote', 'assets/website.png'),
                        Custom('Metacritic', 'assets/website.png'),
                        Custom('Wikipedia', 'assets/website.png'),
                        Custom('TvMaze', 'assets/api.png'),]
    airflow = Airflow('Airflow')

    for source in data_sources:
        extract_edge = Edge()
        source >> extract_edge >> airflow

    load_edge = Edge()
    read_edge = Edge()

    s3 = S3('Analytics s3')
    airflow >> load_edge >> s3

    s3_saving = S3('Raw Titles s3')
    airflow >> Edge() >> s3_saving
    s3_saving >> Edge() >> airflow

    with Cluster(''):
        crawler = GlueCrawlers('Crawler')
        glue = Glue('Glue')
        data_catalog = GlueDataCatalog('Data Catalog')

        s3 >> crawler
        crawler >> glue >> data_catalog

    redshift = Redshift("Redshift")
    data_catalog >> redshift

    slack = Slack("Slack")
    airflow >> slack

    redshift >> Quicksight("Quicksight")
    redshift >> Tableau("Tableau")

    # airflow >> load_edge >> Custom(
    #     'Typesense NoSQL', 'assets/typesense.png')
    airflow >> load_edge >> CosmosDb('Cosmos DB NoSql')

    rdbs = RDS('RDS')
    airflow >> load_edge >> rdbs
    rdbs >> read_edge >> airflow

    s3_saving >> Edge() >> rdbs
