import logging
import concurrent.futures

from scraper import CONFIG
from functools import partial
from collections.abc import Iterator
from azure.core.exceptions import ResourceExistsError
from azure.core.exceptions import ResourceNotFoundError
from azure.core.exceptions import ClientAuthenticationError
from azure.core.exceptions import ServiceResponseError
from azure.core.exceptions import ServiceResponseTimeoutError
from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient
from azure.data.tables import TableClient
from azure.data.tables import TableEntity

logging.getLogger('azure').setLevel(logging.WARNING)
LOGGER = logging.getLogger(__name__)

class TableStorageHandler(object):
    """
    The table storage handler object is used to connect to a table in Azure
    Table Storage. Connection retries and data modifications on the 
    corresponding table via inserting, upserting, and deleting entities are
    handled by this class.

    Attributes
    ----------
    upsert_enabled : bool
        A boolean value indicating whether or not upserting is enabled if 
        the entity already exists in the table.
    retry_entity_creation_enabled : bool
        A boolean value indicating whether or not retrying entity creation
        is enabled if a previous creation attempt has failed.
    retry_entity_creation_count : int
        The number of times to retry entity creation if a previous creation
        attempt has failed.
    retry_entity_extraction_enabled : bool
        A boolean value indicating whether or not retrying entity extraction
        is enabled if a previous extraction attempt has failed.
    retry_entity_extraction_count : int
        The number of times to retry entity extraction if a previous extraction
        attempt has failed.
    table_name : str
        The name of the table in Azure Table Storage account.
    service_table_client : azure.data.tables.TableServiceClient
        The TableServiceClient object connected to the Azure Table Storage.
    table_client : azure.data.tables.TableClient
        The TableClient object connected to the table in Azure Table Storage.
    thread_worker_count : int
        The number of threads to use when writing data to the table in Azure
        Table Storage.

    Methods
    -------
    write_data_to_table(entities: collections.abc.Iterable[azure.data.tables.TableEntity]) -> None
        Writes the given entities to the table in Azure Table Storage.
    try_get_entity(partition_key: str, row_key: str, retries_remaining: int = 0, **kwargs) -> azure.data.tables.TableEntity
        Attempts to get an entity from the table in Azure Table Storage.
    try_query_entities(filter: str, retries_remaining: int = 0, **kwargs) -> azure.data.tables.ItemPaged[azure.data.tables.TableEntity]
        Attempts to query entities from the table in Azure Table Storage.
    """

    configs = CONFIG['StorageHandlerSettings']
    upsert_enabled = configs['UpsertAtFailedPushEnabled']
    retry_entity_creation_enabled = configs['RetryEntityCreationEnabled']
    retry_entity_creation_count = configs['RetryEntityCreationCount']
    retry_entity_extraction_enabled = configs['RetryEntityExtractionEnabled']
    retry_entity_extraction_count = configs['RetryEntityExtractionCount']
    thread_worker_count = configs['ThreadWorkerCount']

    def __init__(
            self,
            table_name: str,
            account_name: str = None, 
            access_key: str = None, 
            connection_string: str = None) -> None:
        """
        Parameters
        ----------
        table_name : str
            The name of the table in Azure Table Storage account.
        account_name : str, optional
            (Default: None) The account name of the Azure Table Storage.
        access_key : str, optional
            (Default: None) The access key of the Azure Table Storage.
        connection_string : str, optional
            (Default: None) The connection string of the Azure Table Storage.
        """

        # Azure Table Storage Client
        self.table_name = table_name
        self.__account_name = account_name
        self.__access_key = access_key
        self.__connection_string = connection_string

        self.table_service_client = self.connect_table_service_client(account_name=account_name, access_key=access_key, connection_string=connection_string)
        self.table_client = self.connect_table_client(table_service_client=self.table_service_client, table_name=self.table_name)

    @staticmethod
    def connect_table_service_client(
            account_name: str = None,
            access_key: str = None,
            connection_string: str = None) -> TableServiceClient:
        """
        Connects to the TableServiceClient object in Azure Table Storage
        based on the given credentials.
        
        Parameters
        ----------
        account_name : str, optional
            (Default: None) The account name of the Azure Table Storage.
        access_key : str, optional
            (Default: None) The access key of the Azure Table Storage.
        connection_string : str, optional
            (Default: None) The connection string of the Azure Table Storage.

        Returns
        -------
        azure.data.tables.TableServiceClient
            The TableServiceClient object connected to the Azure Table Storage.
        """ 

        if ((account_name is None or access_key is None) and \
            connection_string is None):
            LOGGER.error('At least one of (account_name + access_key) or connection_string must contain a value.')

        LOGGER.debug(f'Connecting to table service client.')
        if (connection_string is not None):
            try:
                LOGGER.debug('Attempting connection via connection string.')
                return TableServiceClient.from_connection_string(conn_str=connection_string)
            except Exception as ex:
                LOGGER.error('Connection attempt via connection string failed.')
                LOGGER.error(str(ex))
        
        if (account_name is not None and access_key is not None):
            try:
                LOGGER.debug('Attempting connection via account name and access key.')
                credential = AzureNamedKeyCredential(account_name, access_key)
                return TableServiceClient(endpoint=f'https://{account_name}.table.core.windows.net/', credential=credential)
            except Exception as ex:
                LOGGER.error('Connection attempt via account name and access key failed.')
                LOGGER.error(str(ex))

    @staticmethod
    def connect_table_client(table_service_client: TableServiceClient, table_name:str) -> TableClient:
        """
        Connects to the TableClient object in Azure Table Storage based on
        the given table name.

        Parameters
        ----------
        table_service_client : azure.data.tables.TableServiceClient
            The TableServiceClient object connected to the Azure Table Storage.
        table_name : str
            The name of the table in Azure Table Storage account.

        Returns
        -------
        azure.data.tables.TableClient
            The TableClient object connected to the table in Azure Table Storage.
        """

        try:
            LOGGER.debug(f'Connecting to table client {table_name}.')
            return table_service_client.create_table_if_not_exists(table_name=table_name)
        except Exception as ex:
            LOGGER.error(f'Failed to connect to table client {table_name}.')
            LOGGER.error(str(ex))

    @classmethod
    def try_create_or_upsert_entity_with_retry(
            self,
            entity: TableEntity, 
            retries_remaining: int = 0,
            **kwargs) -> str:
        """
        Attempts to create or upsert the given entity in the table in Azure
        Table Storage. If the entity already exists, it will be updated with
        the new values. If the entity does not exist, it will be created.

        This function will be retried if a client authentication error occurs.
        
        Parameters
        ----------
        entity : azure.data.tables.TableEntity
            The entity to be created or upserted in the table in Azure Table Storage.
        retries_remaining : int, optional
            (Default: 0) The number of retries remaining to attempt to create
            or upsert the entity in the table in Azure Table Storage.
        
        Returns
        -------
        str
            A message indicating the result of the operation.
        """

        assert retries_remaining >= 0, 'Retries remaining must be greater than or equal to 0.'

        table_client: TableClient = kwargs.get('table_client', None)
        table_name: str = kwargs.get('table_name', None)

        account_name: str = kwargs.get('account_name', None)
        access_key: str = kwargs.get('access_key', None)
        connection_string: str = kwargs.get('connection_string', None)


        try:
            LOGGER.debug(f'Attempting to create or upsert entity {entity["PartitionKey"]} in {kwargs.get("table_name", None)}.')
            table_client.create_entity(entity=entity)
            return f'Created entity with PartitionKey {entity["PartitionKey"]} and RowKey {entity["RowKey"]} in {kwargs.get("table_name", None)}.'
        except ResourceExistsError:
            LOGGER.debug(f'Entity {entity["PartitionKey"]} already exists in {table_name}.')

            if self.upsert_enabled:
                try:
                    LOGGER.debug(f'Attempting upsert of entity {entity["PartitionKey"]} to {table_name}')
                    table_client.upsert_entity(entity=entity)
                    return f'Upserted entity with PartitionKey {entity["PartitionKey"]} and RowKey {entity["RowKey"]} in {kwargs.get("table_name", None)}.'
                except Exception as ex:
                    LOGGER.error('Failed to upsert entity.')
                    LOGGER.error(str(ex))
        except (ClientAuthenticationError, ServiceResponseError, ServiceResponseTimeoutError) as ex:
            LOGGER.error(f'Encountered {type(ex)}, attempting re-login and retry entity creation.')
            LOGGER.error(str(ex))
            table_service_client = self.connect_table_service_client(account_name=account_name, access_key=access_key, connection_string=connection_string)
            table_client = self.connect_table_client(table_service_client=table_service_client, table_name=table_name)

            if self.retry_entity_creation_enabled and retries_remaining > 0:
                LOGGER.debug(f'Retrying entity creation {retries_remaining} more times.')
                self.try_create_or_upsert_entity_with_retry(
                    entity=entity, 
                    retries_remaining=retries_remaining-1,
                    table_client=table_client,
                    table_name=table_name,
                    account_name=account_name,
                    access_key=access_key,
                    connection_string=connection_string)
            else:
                LOGGER.debug('Entity creation retry limit reached / Retry not enabled, skipping entity creation.')

    def write_data_to_table(self, entities: Iterator[TableEntity]) -> None:
        """
        Writes the given entities to the table in Azure Table Storage.

        Parameters
        ----------
        entities : collections.abc.Iterable[azure.data.tables.TableEntity]
            The entities to be written to the table in Azure Table Storage.

        Returns
        -------
        None
        """
        
        LOGGER.debug(f'Writing entities to the table {self.table_name}.')
        try_create_or_upsert_entity = partial(
            self.try_create_or_upsert_entity_with_retry, 
            table_client=self.table_client, 
            table_name=self.table_name, 
            account_name=self.__account_name, 
            access_key=self.__access_key, 
            connection_string=self.__connection_string,
            retries_remaining=self.retry_entity_creation_count)
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.thread_worker_count) as executor:
            LOGGER.debug(f'Running thread executor with at most {executor._max_workers} threads.')
            try:
                results = executor.map(try_create_or_upsert_entity, entities)
                for result in results:
                    LOGGER.debug(result)
            except Exception as ex:
                LOGGER.error(str(ex))
                LOGGER.error(f'Exception type: {type(ex)}')
            LOGGER.debug(f'Pool threads complete.')
        LOGGER.debug(f'Sent all entities to table {self.table_name}.')

    def try_get_entity(
            self, 
            partition_key: str,
            row_key: str,
            retries_remaining: int = 0,
            **kwargs) -> TableEntity:
        """
        Attempts to get the entity with the given partition key and row key.

        Parameters
        ----------
        partition_key : str
            The partition key of the entity to be retrieved.
        row_key : str
            The row key of the entity to be retrieved.
        retries_remaining : int, optional
            (Default: 0) The number of retries remaining to attempt to get
            the entity with the given partition key and row key.
        **kwargs
            Additional keyword arguments to pass to the get_entity method.

        Returns
        -------
        azure.data.tables.TableEntity
            The entity with the given partition key and row key.
        """

        assert retries_remaining >= 0, 'Retries remaining must be greater than or equal to 0.'

        try:
            LOGGER.debug(f'Attempting to get entity with partition key {partition_key} and row key {row_key}.')
            return self.table_client.get_entity(partition_key=partition_key, row_key=row_key, **kwargs)
        except ResourceNotFoundError as ex:
            LOGGER.debug(f'Entity with partition key {partition_key} and row key {row_key} not found.')
            LOGGER.debug(str(ex))
            return None
        except ClientAuthenticationError as ex:
            LOGGER.error('Client authentication error encountered, attempting re-login and retry entity extraction.')
            LOGGER.error(str(ex))
            self.table_service_client = self.connect_table_service_client(account_name=self.__account_name, access_key=self.__access_key, connection_string=self.__connection_string)
            self.table_client = self.connect_table_client(table_service_client=self.table_service_client, table_name=self.table_name)

            if self.retry_entity_extraction_enabled and retries_remaining > 0:
                LOGGER.debug(f'Retrying entity extraction {retries_remaining} more times.')
                self.try_get_entity(partition_key=partition_key, row_key=row_key, retries_remaining=retries_remaining-1, **kwargs)
            else:
                LOGGER.debug('Entity extraction retry limit reached / Retry not enabled, skipping entity creation.')

    def try_query_entities(
            self, 
            query_filter: str,
            retries_remaining: int = 0,
            **kwargs) -> Iterator[TableEntity]:
        """
        Attempts to query the entities in the table with the given filter.

        Parameters
        ----------
        query_filter : str
            The filter to be used to query the entities in the table.
        retries_remaining : int, optional
            (Default: 0) The number of retries remaining to attempt to query
            the entities in the table with the given filter.
        **kwargs
            Additional keyword arguments to pass to the query_entities method.

        Returns
        -------
        list[azure.data.tables.TableEntity]
            The entities in the table with the given filter.
        """

        assert retries_remaining >= 0, 'retries_remaining must be greater than or equal to 0.'

        try:
            LOGGER.debug(f'Attempting to query entities with filter {query_filter}.')
            return self.table_client.query_entities(query_filter=query_filter, **kwargs)
        except ClientAuthenticationError as ex:
            LOGGER.error('Client authentication error encountered, attempting re-login and retry entity extraction.')
            LOGGER.error(str(ex))
            self.table_service_client = self.connect_table_service_client(account_name=self.__account_name, access_key=self.__access_key, connection_string=self.__connection_string)
            self.table_client = self.connect_table_client(table_service_client=self.table_service_client, table_name=self.table_name)

            if self.retry_entity_extraction_enabled and retries_remaining > 0:
                LOGGER.debug(f'Retrying entity extraction {retries_remaining} more times.')
                self.try_query_entities(query_filter=query_filter, retries_remaining=retries_remaining-1, **kwargs)
            else:
                LOGGER.debug('Entity extraction retry limit reached / Retry not enabled, skipping entity creation.')