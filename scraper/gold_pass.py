import logging
import coc
import datetime

from scraper import CONFIG
from scraper.storage import StorageHandler

LOGGER = logging.getLogger(__name__)
    
class GoldPassTableHandler(StorageHandler):

    configs = CONFIG['GoldPassSettings']
    table = configs['TableName']
    scrape_enabled = configs['ScrapeEnabled']
    scrape_freq = configs['ScrapeFrequency']
    partition_key = configs['PartitionKey']
    row_key = configs['RowKey']

    def __init__(self, coc_client:coc.Client, **kwargs) -> None:
        super().__init__(self.table, **kwargs)
        self.coc_client = coc_client

    def __convert_timedelta_to_days__(self, dt):
        return dt / datetime.timedelta(days=1)

    def __convert_data_to_entity_list__(self, data):
        LOGGER.debug(f'Creating entity for Gold Pass Season.')
        entity = dict()
        entity['PartitionKey'] = self.partition_key
        entity['RowKey'] = self.row_key
        entity['SeasonId'] = datetime.datetime.now().strftime('%Y-%m')
        entity['StartTime'] = data.start_time.time
        entity['EndTime'] = data.end_time.time
        entity['Duration'] = self.__convert_timedelta_to_days__(data.duration)
        yield entity

    async def __update_table__(self) -> None:
        LOGGER.debug('Scraping Gold Pass data.')
        data = await self.coc_client.get_current_goldpass_season()
        entities = self.__convert_data_to_entity_list__(data)
        self.__write_data_to_table__(entities=entities)

    async def process_table(self) -> None:
        if self.scrape_enabled:
            LOGGER.debug(f'Gold Pass table {self.table} is updating.')
            await self.__update_table__()
        else:
            LOGGER.debug(f'Gold Pass table {self.table} is not updated because GoldPassSettings.ScrapeEnabled is {self.scrape_enabled}.')