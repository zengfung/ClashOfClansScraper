import pytest

from ..scraper.storage import TableStorageHandler
from azure.data.tables import TableServiceClient
from azure.data.tables import TableClient

class TestTableStorageConnection:
    dummy_table_name: str = 'TestTable'
    dummy_fail_account_name: str = 'dummy'
    dummy_fail_access_key: str = 'dummy'
    dummy_fail_connection_string: str = 'dummy'

    def test_connect_table_service_client_success__name_and_access_key(self, name: str, access_key: str):
        client = TableStorageHandler.connect_table_service_client(account_name=name, access_key=access_key)
        assert isinstance(client, TableServiceClient)

    def test_connect_table_service_client_success__connection_string(self, connection_string: str):
        client = TableStorageHandler.connect_table_service_client(connection_string=connection_string)
        assert isinstance(client, TableServiceClient)

    def test_connect_table_service_client_success__all(self, name: str, access_key: str, connection_string: str):
        client = TableStorageHandler.connect_table_service_client(account_name=name, access_key=access_key, connection_string=connection_string)
        assert isinstance(client, TableServiceClient)

    def test_connect_table_service_client_failure__name_and_access_key(self):
        with pytest.raises(Exception):
            client = TableStorageHandler.connect_table_service_client(account_name=self.dummy_fail_account_name, access_key=self.dummy_fail_access_key)
            assert client is None

    def test_connect_table_service_client_failure__connection_string(self):
        client = TableStorageHandler.connect_table_service_client(connection_string=self.dummy_fail_connection_string)
        assert client is None

    def test_connect_table_service_client_failure__all(self):
        with pytest.raises(Exception):
            client = TableStorageHandler.connect_table_service_client(account_name=self.dummy_fail_account_name, access_key=self.dummy_fail_access_key, connection_string=self.dummy_fail_connection_string)
            assert client is None

    def test_connect_table_service_client_failure__name_only(self, name: str):
        client = TableStorageHandler.connect_table_service_client(account_name=name)
        assert client is None

    def test_connect_table_service_client_failure__no_args(self):
        client = TableStorageHandler.connect_table_service_client()
        assert client is None

    def test_connect_table_client_success__table_name(self, name: str, access_key: str):
        table_service_client = TableStorageHandler.connect_table_service_client(account_name=name, access_key=access_key)
        table_client = TableStorageHandler.connect_table_client(table_service_client=table_service_client, table_name=self.dummy_table_name)
        assert isinstance(table_client, TableClient)
