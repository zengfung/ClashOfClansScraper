import logging

from scraper import CONFIG
from typing import Dict
from typing import Union
from azure.core.exceptions import ResourceExistsError
from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient

LOGGER = logging.getLogger(__name__)

class StorageHandler(object):

    configs = CONFIG['StorageHandlerSettings']
    upsert_enabled = configs['UpsertAtFailedPushEnabled']

    def __init__(self,
                 table_name:str, 
                 account_name:str = None, 
                 access_key:str = None, 
                 connection_string:str = None) -> None:
        self.service_table_client = self.__connect_table_service_client__(account_name=account_name, access_key=access_key, connection_string=connection_string)
        self.table_client = self.__connect_table_client__(table_name)

    def __connect_table_service_client__(self,
                                         account_name:str = None,
                                         access_key:str = None,
                                         connection_string:str = None) -> TableServiceClient:
        if ((account_name is None or access_key is None) and \
            connection_string is None):
            LOGGER.error('At least one of (account_name + access_key) or connection_string must contain a value.')

        LOGGER.debug(f'Connecting to table service client.')
        if (connection_string is not None):
            try:
                LOGGER.info('Attempting connection via connection string.')
                return TableServiceClient.from_connection_string(conn_str=connection_string)
            except:
                LOGGER.info('Connection attempt via connection string failed.')
        
        if (account_name is not None and access_key is not None):
            try:
                LOGGER.info('Attempting connection via account name and access key.')
                credential = AzureNamedKeyCredential(account_name, access_key)
                return TableServiceClient(endpoint=f'https://{account_name}.table.core.windows.net/', credential=credential)
            except:
                LOGGER.info('Connection attempt via account name and access key failed.')

    def __connect_table_client__(self, name:str) -> None:
        LOGGER.info(f'Connecting to table client {name}')
        return self.service_table_client.create_table_if_not_exists(table_name=name)

    def __write_data_to_table__(self, entities: Dict[str,Union[float,int,str]]) -> None:
        LOGGER.debug(f'Writing entities to the table {self.table_client}.')
        for entity in entities:
            try:
                self.table_client.create_entity(entity=entity)
                LOGGER.debug('Successfully written entity to table.')
            except ResourceExistsError:
                LOGGER.warning(f'Entity {entity["PartitionKey"]} already exists in {self.table_client}.')

                if self.upsert_enabled:
                    try:
                        LOGGER.debug(f'Attempting upsert of entity {entity["PartitionKey"]} to {self.table_client}')
                        self.table_client.upsert_entity(entity=entity)
                        LOGGER.debug('Successfully upsert entity.')
                    except Exception as ex:
                        LOGGER.error('Failed to upsert entity.')
                        LOGGER.error(str(ex))
        LOGGER.debug('Sent all entities to table.')
    