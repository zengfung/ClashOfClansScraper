import logging
import coc
import datetime

from scraper import CONFIG
from scraper.players import PlayerTableHandler
from scraper.clans import ClanTableHandler
from scraper.storage import StorageHandler
from scraper.utils import try_get_attr
from collections.abc import Iterator
from azure.data.tables import TableEntity

LOGGER = logging.getLogger(__name__)

class LocationTableHandler(StorageHandler):
    """
    The table contains a single entity for each location. The entity's
    PartitionKey is the location's ID and the RowKey is the year-month
    of the scrape. This is to ensure that the IDs that represent the
    same location are kept constant over time.

    The table is updated every month.

    Attributes
    ----------
    locations : List[int]
        The list of locations to scrape.
    scrape_enabled : bool
        Whether or not to scrape the locations.
    player_scrape_by_location_enabled : bool
        Whether or not to scrape the players in the locations.
    player_scrape_limit : int
        The number of players to scrape in each location.
    clan_scrape_by_location_enabled : bool
        Whether or not to scrape the clans in the locations.
    clan_scrape_limit : int
        The number of clans to scrape in each location.
    abandon_scrape_if_entity_exists : bool
        Determines if the scrape should be abandoned if the entity exists in
        the table.

    Methods
    -------
    process_clan_scrape(coc_client_handling: bool = True) -> None
        Scrapes the clans in the locations.
    process_player_scrape(coc_client_handling: bool = True) -> None
        Scrapes the players in the locations.
    process_table(coc_client_handling: bool = True) -> None
        Updates the location table in the database.
    """

    configs = CONFIG['LocationSettings']
    locations = configs['Locations']
    scrape_enabled = configs['ScrapeEnabled']
    scrape_from_all_locations_enabled = configs['ScrapeFromAllLocationsEnabled']
    clan_scrape_by_location_enabled = configs["ClanScrapeByLocationEnabled"]
    clan_scrape_limit = configs["ClanScrapeLimit"]
    player_scrape_by_location_enabled = configs["PlayerScrapeByLocationEnabled"]
    player_scrape_limit = configs["PlayerScrapeLimit"]
    abandon_scrape_if_entity_exists = configs['AbandonScrapeIfEntityExists']

    def __init__(self, coc_client: coc.Client = None, **kwargs) -> None:
        """
        Parameters
        ----------
        coc_client : coc.Client
            (Default: None) The Clash of Clans API client object.
        kwargs
            The kwargs used to initialize the StorageHandler.
        """

        super().__init__(table_name=self.configs['TableName'], coc_client=coc_client, **kwargs)
        self.__login_kwargs = kwargs

    def __get_row_key(self) -> str:
        """
        Gets the row key for the location.

        Returns
        -------
        str
            The row key for the clan in the current month.
        """

        return datetime.datetime.now().strftime('%Y-%m')

    def __does_location_data_exist(self) -> bool:
        """
        Checks if the location data exists in the table. The location data
        for the current month exists if the current month's row key exists
        for any location in the table.

        Returns
        -------
        bool
            True if the location data exists in the table, False otherwise.
        """

        LOGGER.debug(f'Checking if location data with row key {self.__get_row_key()} exists in table {self.table_name}.')
        
        row_key = self.__get_row_key()

        query_filter = f"RowKey eq '{row_key}'"
        results = self.try_query_entities(query_filter=query_filter, retries_remaining=self.retry_entity_extraction_count, select='PartitionKey')
        
        has_results = bool(next(results, False))
        return has_results

    def __get_location_ids(self, locations: Iterator[coc.Location], only_country_ids: bool = True) -> Iterator[str]:
        """
        Gets the location IDs from the locations.

        Parameters
        ----------
        locations : Iterator[coc.Location]
            The locations whose tags need to be retrieved.
        only_country_ids : bool
            Whether or not to only get the IDs for country locations.

        Yields
        ------
        str
            The list of location IDs.
        """

        for location in locations:
            if only_country_ids and not try_get_attr(location, 'is_country', default=False):
                continue

            LOGGER.debug(f"Getting location id {try_get_attr(location, 'id')}.")
            yield try_get_attr(location, 'id')

    def __convert_data_to_entity_list__(self, locations: Iterator[coc.Location]) -> Iterator[TableEntity]:
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
            entity['RowKey'] = self.__get_row_key()
            entity['Id'] = try_get_attr(location, 'id')
            entity['Name'] = try_get_attr(location, 'name')
            entity['IsCountry'] = try_get_attr(location, 'is_country')
            entity['CountryCode'] = try_get_attr(location, 'country_code', default='')
            entity['LocalizedName'] = try_get_attr(location, 'localized_name', default='')

            LOGGER.info(entity)
            yield entity

    async def process_table(self, coc_client_handling: bool = True) -> None:
        """
        Updates the location table in the database.

        Parameters
        ----------
        coc_client_handling : bool, optional
            (Default: True) Whether or not to handle the coc client session
            automatically.

        Returns
        -------
        None
        """

        should_abandon_scrape = self.abandon_scrape_if_entity_exists and self.__does_location_data_exist()
        if should_abandon_scrape:
            LOGGER.info(f'Abandoning scrape for {self.table_name} table because location data with row key {self.__get_row_key()} already exists.')
            return None

        if coc_client_handling:
            await self.start_coc_client_session()

        if self.scrape_enabled:
            try:
                LOGGER.info("Scraping location data...")
                
                locations = await self.coc_client.search_locations(limit=None)
                entities = self.__convert_data_to_entity_list__(locations)
                self.write_data_to_table(entities=entities)
                    
                LOGGER.info("Location data scraped successfully.")
            except Exception as ex:
                LOGGER.error("Error occurred while scraping location data.")
                LOGGER.error(str(ex))

        if coc_client_handling:
            await self.close_coc_client_session()

    async def process_clan_scrape(self, coc_client_handling: bool = True) -> None:
        """
        Scrapes the clans in the locations specified in the config file.

        Parameters
        ----------
        coc_client_handling : bool, optional
            (Default: True) Whether or not to handle the coc client session
            automatically.

        Returns
        -------
        None
        """

        if coc_client_handling:
            await self.start_coc_client_session()

        if self.clan_scrape_by_location_enabled:
            try:
                if self.scrape_from_all_locations_enabled:
                    LOGGER.info("Scraping clans from all locations.")
                    locations = await self.coc_client.search_locations(limit=None)
                    location_ids = self.__get_location_ids(locations, only_country_ids=False)
                else:
                    LOGGER.info("Scraping clans from specified locations.")
                    location_ids = self.locations

                LOGGER.info("Scraping clans by location.")
                for location in location_ids:
                    LOGGER.info(f"Scraping {self.clan_scrape_limit} clans in {location}.")
                    clans = await self.coc_client.get_location_clans(location_id=location, limit=self.clan_scrape_limit)
                    writer = ClanTableHandler(coc_client=self.coc_client, **self.__login_kwargs)
                    await writer.scrape_location_clans(clans, coc_client_handling=False)
            except Exception as ex:
                LOGGER.error("Error occurred while scraping clans by location.")
                LOGGER.error(str(ex))

        if coc_client_handling:
            await self.close_coc_client_session()

    async def process_player_scrape(self, coc_client_handling: bool = True) -> None:
        """
        Scrapes the players in the locations specified in the config file.

        Parameters
        ----------
        coc_client_handling : bool, optional
            (Default: True) Whether or not to handle the coc client session
            automatically.

        Returns
        -------
        None
        """

        if coc_client_handling:
            await self.start_coc_client_session()

        if self.player_scrape_by_location_enabled:
            try:
                if self.scrape_from_all_locations_enabled:
                    LOGGER.info("Scraping players from all locations.")
                    locations = await self.coc_client.search_locations(limit=None)
                    location_ids = self.__get_location_ids(locations, only_country_ids=True)
                else:
                    LOGGER.info("Scraping players from specified locations.")
                    location_ids = self.locations

                LOGGER.info("Scraping players by location.")
                for location in location_ids:
                    LOGGER.info(f"Scraping {self.player_scrape_limit} players in {location}.")
                    players = await self.coc_client.get_location_players(location_id=location, limit=self.player_scrape_limit)
                    writer = PlayerTableHandler(coc_client=self.coc_client, **self.__login_kwargs)
                    await writer.scrape_location_players(players, coc_client_handling=False)
            except Exception as ex:
                LOGGER.error("Error occurred while scraping players by location.")
                LOGGER.error(str(ex))

        if coc_client_handling:
            await self.close_coc_client_session()
