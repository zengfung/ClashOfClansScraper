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
            
            # RowKey is currently set as year-month of scrape. This is to 
            # ensure that the IDs that represent the same location are
            # kept constant over time.
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

    async def process_table(self) -> None:
        """
        Updates the location table in the database.
        """

        if self.scrape_enabled:
            try:
                LOGGER.info("Scraping location data...")
                
                locations = await self.coc_client.search_locations(limit=None)
                entities = self.__convert_data_to_entity_list__(locations)
                self.__write_data_to_table__(entities=entities)
                    
                LOGGER.info("Location data scraped successfully.")
            except Exception as ex:
                LOGGER.error("Error occurred while scraping location data.")
                LOGGER.error(str(ex))

    async def process_clan_scrape(self) -> None:
        """
        Scrapes the clans in the locations specified in the config file.
        """

        if self.clan_scrape_by_location_enabled:
            try:
                LOGGER.info("Scraping clans by location.")
                for location in self.locations:
                    LOGGER.info(f"Scraping {self.clan_scrape_limit} clans in {location}.")
                    clans = await self.coc_client.get_location_clans(location_id=location, limit=self.clan_scrape_limit)
                    writer = ClanTableHandler(self.coc_client, **self.kwargs)
                    await writer.scrape_location_clans(clans)
            except Exception as ex:
                LOGGER.error("Error occurred while scraping clans by location.")
                LOGGER.error(str(ex))

    async def process_player_scrape(self) -> None:
        """
        Scrapes the players in the locations specified in the config file.
        """

        if self.player_scrape_by_location_enabled:
            try:
                LOGGER.info("Scraping players by location.")
                for location in self.locations:
                    LOGGER.info(f"Scraping {self.player_scrape_limit} players in {location}.")
                    players = await self.coc_client.get_location_players(location_id=location, limit=self.player_scrape_limit)
                    writer = PlayerTableHandler(self.coc_client, **self.kwargs)
                    writer.scrape_location_players(players)
            except Exception as ex:
                LOGGER.error("Error occurred while scraping players by location.")
                LOGGER.error(str(ex))
