import logging
import coc
import datetime

from scraper import CONFIG
from scraper.players import PlayerTableHandler
from scraper.clans import ClanTableHandler
from scraper.storage import StorageHandler
from scraper.utils import try_get_attr
from typing import List
from typing import Union
from typing import Generator
from typing import Dict

LOGGER = logging.getLogger(__name__)

class LocationTableHandler(StorageHandler):
    """
    """

    configs = CONFIG['LocationSettings']
    table = configs['TableName']
    locations = configs['Locations']
    scrape_enabled = configs['ScrapeEnabled']
    clan_scrape_by_location_enabled = configs["ClanScrapeByLocationEnabled"]
    clan_scrape_limit = configs["ClanScrapeLimit"]
    player_scrape_by_location_enabled = configs["PlayerScrapeByLocationEnabled"]
    player_scrape_limit = configs["PlayerScrapeLimit"]

    def __init__(self, coc_client: coc.Client, **kwargs) -> None:
        """
        Parameters
        ----------
        coc_client : coc.Client
            The coc client used to connect to the Clash of Clans API.
        kwargs : Dict[str,Union[str,bool]]
            The kwargs used to initialize the StorageHandler.
        """

        super().__init__(self.table, **kwargs)
        self.coc_client = coc_client
        self.kwargs = kwargs

    def __convert_data_to_entity_list__(self, locations: List[coc.Location]) -> Generator[Dict[str,Union[float,int,str]],None,None]:
        """
        Converts the location's data to a list of entities.

        Parameters
        ----------
        locations : List[coc.Location]
            The list of locations whose data needs to be converted.

        Yields
        ------
        Dict[str,Union[float,int,str]]
            The entity containing the location's data.
        """

        LOGGER.info("Converting location data to entities.")
        for location in locations:
            LOGGER.info(f"Converting location data for {try_get_attr(location, 'name')} with ID {try_get_attr(location, 'id')}.")
            entity = dict()
            entity['PartitionKey'] = f"{try_get_attr(location, 'id')}"
            entity['RowKey'] = f'{datetime.datetime.now().strftime("%Y-%m")}'
            entity['Id'] = try_get_attr(location, 'id')
            entity['Name'] = try_get_attr(location, 'name')
            entity['IsCountry'] = try_get_attr(location, 'is_country')
            entity['CountryCode'] = try_get_attr(location, 'country_code', default='')
            entity['LocalizedName'] = try_get_attr(location, 'localized_name', default='')

            LOGGER.info(entity)
            yield entity

    async def update_location_table(self) -> None:
        """
        Updates the location table in the database.
        """

        if self.scrape_enabled:
            LOGGER.info("Scraping location data...")
            
            locations = await self.coc_client.search_locations(limit=None)
            entities = self.__convert_data_to_entity_list__(locations)
            self.__write_data_to_table__(entities=entities)
                
            LOGGER.info("Location data scraped successfully.")
