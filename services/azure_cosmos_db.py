import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
from decouple import config


HOST = config('ACCOUNT_HOST')
MASTER_KEY = config('ACCOUNT_KEY')
DATABASE_ID = config('COSMOS_DATABASE')
CONTAINER_ID = config('COSMOS_CONTAINER')


class CosmosDbConnection:
    def __enter__(self):
        client = cosmos_client.CosmosClient(
            HOST, {'masterKey': MASTER_KEY}, user_agent="TELEVISIONS_UA", user_agent_overwrite=True)
        try:
            db = client.create_database(id=DATABASE_ID)
            print('Database with id \'{0}\' created'.format(DATABASE_ID))

        except exceptions.CosmosResourceExistsError:
            db = client.get_database_client(DATABASE_ID)
            print('Database with id \'{0}\' was found'.format(DATABASE_ID))

        try:
            container = db.create_container(
                id=CONTAINER_ID, partition_key=PartitionKey(path='/partitionKey'))
            print('Container with id \'{0}\' created'.format(CONTAINER_ID))

        except exceptions.CosmosResourceExistsError:
            container = db.get_container_client(CONTAINER_ID)
            print('Container with id \'{0}\' was found'.format(CONTAINER_ID))

        self.db = db
        self.container = container
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass
