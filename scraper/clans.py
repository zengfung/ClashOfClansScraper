import logging
import coc
import datetime

from collections.abc import Iterator
from scraper import CONFIG
from scraper.coc_client import CocClientHandler
from scraper.players import PlayerTableHandler
from scraper.storage import TableStorageHandler
from scraper.utils import try_get_attr
from azure.data.tables import TableEntity

LOGGER = logging.getLogger(__name__)

class ClanTableHandler(CocClientHandler):
    """
    The table contains a clan's current progress in the game.

    Attributes
    ----------
    table_name : str
        The name of the table in Azure Table Storage.
    clans : list[str]
        The list of clan tags whose data needs to be scraped.
    scrape_enabled : bool
        Whether clan data should be scraped or not.
    member_scrape_enabled : bool
        Whether clan member data should be scraped or not.
    abandon_scrape_if_entity_exists : bool
        Determines if the scrape should be abandoned if the entity exists in
        the table.

    Methods
    -------
    scrape_location_clans(clans: List[coc.clans.RankedClan]) -> None
        Scrapes all the clans' data from a specific location.
    process_table() -> None
        Processes the clan table in the database.
    """

    configs = CONFIG['ClanSettings']
    table_name = configs['TableName']
    clans = configs['Clans']
    scrape_enabled = configs['ScrapeEnabled']
    member_scrape_enabled = configs['MemberScrapeEnabled']
    abandon_scrape_if_entity_exists = configs['AbandonScrapeIfEntityExists']

    def __init__(
            self, 
            coc_email: str,
            coc_password: str,
            coc_client: coc.Client = None,
            **kwargs) -> None:
        """
        Parameters
        ----------
        coc_email : str
            The email address of the Clash of Clans account.
        coc_password : str
            The password of the Clash of Clans account.
        coc_client : coc.Client, optional
            (Default: None) The Clash of Clans client to use.
        **kwargs
            Keyword arguments to pass to the TableStorageHandler class.
        """
        
        super().__init__(coc_email=coc_email, coc_password=coc_password, coc_client=coc_client)
        self.table_handler = TableStorageHandler(table_name=self.table_name, **kwargs)
        self.__login_kwargs = kwargs

    def __get_member_tags(self, clan: coc.Clan) -> Iterator[str]:
        """
        Returns an iterator of the clan's member tags.

        Parameters
        ----------
        clan : coc.Clan
            The clan whose member tags need to be scraped.

        Yields
        ------
        str
            The clan's member tags.
        """

        assert isinstance(clan, coc.Clan)

        for member in try_get_attr(clan, 'members'):
            yield try_get_attr(member, 'tag')

    def __get_partition_key(self, clan_tag: str) -> str:
        """
        Gets the partition key for the clan, i.e. the clan tag.

        Parameters
        ----------
        clan_tag : str
            The clan tag whose partition key needs to be retrieved.

        Returns
        -------
        str
            The partition key for the clan.
        """

        return clan_tag.lstrip('#')

    def __get_row_key(self) -> str:
        """
        Gets the row key for the clan.

        Returns
        -------
        str
            The row key for the clan in the current month.
        """

        return datetime.datetime.now().strftime('%Y-%m-%d')

    def __does_clan_data_exist(self, clan_tag: str) -> bool:
        """
        Determines if the clan's data already exists in the table.

        Parameters
        ----------
        clan_tag : str
            The clan tag whose data needs to be checked.

        Returns
        -------
        bool
            True if the clan's data exists in the table, else False.
        """

        LOGGER.debug(f'Checking if entity exists in table {self.table_name}.')
        partition_key = self.__get_partition_key(clan_tag)
        row_key = self.__get_row_key()
        entity = self.table_handler.try_get_entity(partition_key, row_key, select='PartitionKey', retries_remaining=self.table_handler.retry_entity_extraction_count)
        return entity is not None

    async def __scrape_members_data(self, clan: coc.Clan) -> None:
        """
        Scrapes the clan's member data by calling PlayerTableHandler.
        
        Parameters
        ----------
        clan : coc.Clan
            The clan whose member data needs to be scraped.

        Returns
        -------
        None
        """

        assert isinstance(clan, coc.Clan)

        coc_email = self.__login_kwargs.get('coc_email')
        coc_password = self.__login_kwargs.get('coc_password')
        scraper = PlayerTableHandler(coc_email=coc_email, coc_password=coc_password, coc_client=self.coc_client, **self.__login_kwargs)
        member_tags = self.__get_member_tags(clan)
        await scraper.scrape_clan_members(member_tags, coc_client_handling=False)

    async def __scrape_members_data_if_needed(self, clan: coc.Clan) -> None:
        """
        Scrapes the clan's member data if ClanSettings.MemberScrapeEnabled 
        is True.
        
        Parameters
        ----------
        clan : coc.Clan
            The clan whose member data needs to be scraped.

        Returns
        -------
        None
        """

        if self.member_scrape_enabled:
            assert isinstance(clan, coc.Clan)

            LOGGER.debug(f'Scraping members data for clan {try_get_attr(clan, "tag")}.')
            await self.__scrape_members_data(clan)
        else:
            LOGGER.debug(f'Not scraping members data for clan {try_get_attr(clan, "tag")} because ClanSettings.MemberScrapeEnabled is {self.member_scrape_enabled}.')

    def __convert_data_to_entity_list(self, clan: coc.Clan) -> Iterator[TableEntity]:
        """
        Converts the given data to a list of entities to insert/upsert to
        the table.

        Parameters
        ----------
        clan : coc.Clan
            The clan whose data needs to be converted.

        Yields
        -------
        azure.data.tables.TableEntity
            The entities to insert/upsert to the table.
        """

        entity = TableEntity()

        entity['PartitionKey'] = self.__get_partition_key(try_get_attr(clan, 'tag'))
        entity['RowKey'] = self.__get_row_key()
        entity['Name'] = try_get_attr(clan, 'name')
        entity['Tag'] = try_get_attr(clan, 'tag')
        entity['Level'] = try_get_attr(clan, 'level')
        entity['Type'] = try_get_attr(clan, 'type')
        entity['Description'] = try_get_attr(clan, 'description')
        entity['Location'] = try_get_attr(clan.location, 'id') if hasattr(clan, 'location') else None
        entity['Points'] = try_get_attr(clan, 'points')
        entity['VersusPoints'] = try_get_attr(clan, 'versus_points')
        entity['RequiredTrophies'] = try_get_attr(clan, 'required_trophies')
        entity['WarFrequency'] = try_get_attr(clan, 'war_frequency')
        entity['WarWinStreak'] = try_get_attr(clan, 'war_win_streak')
        entity['WarWins'] = try_get_attr(clan, 'war_wins')
        entity['WarTies'] = try_get_attr(clan, 'war_ties')
        entity['WarLosses'] = try_get_attr(clan, 'war_losses')
        entity['IsWarLogPublic'] = try_get_attr(clan, 'is_war_log_public')
        entity['MemberCount'] = try_get_attr(clan, 'member_count')

        yield entity

    def __update_table(self, clan: coc.Clan) -> None:
        """
        Updates the table with the clan's data.

        Parameters
        ----------
        clan : coc.Clan
            The clan whose data needs to be updated.

        Returns
        -------
        None
        """

        entities = self.__convert_data_to_entity_list(clan)
        self.table_handler.write_data_to_table(entities=entities)

    async def scrape_location_clans(self, clans: list[coc.clans.RankedClan], coc_client_handling: bool = True) -> None:
        """
        Scrapes the given location's clans.

        Parameters
        ----------
        clan_tags : list[RankedClan]
            The list of ranked clans to scrape.
        coc_client_handling : bool, optional
            (Default: True) Whether or not to handle the coc client session
            automatically.

        Returns
        -------
        None
        """

        if coc_client_handling:
            await self.start_coc_client_session()

        for clan in clans:
            # Abandon scrape of clan if the clan data already exists.
            should_abandon_scrape = self.abandon_scrape_if_entity_exists and self.__does_clan_data_exist(try_get_attr(clan, 'tag'))
            if should_abandon_scrape:
                LOGGER.info(f'Abandoning clan scrape for the clan {try_get_attr(clan, "tag")} because the clan data already exists.')
                continue

            try:
                LOGGER.debug(f'Scraping clan {try_get_attr(clan, "tag")}.')
                clan = await self.coc_client.get_clan(try_get_attr(clan, 'tag')) if not isinstance(clan, coc.Clan) else clan
                self.__update_table(clan)
                await self.__scrape_members_data_if_needed(clan)
            except Exception as ex:
                LOGGER.error(f'Error while scraping clan {try_get_attr(clan, "tag")}: {ex}')
                LOGGER.error(str(ex))

        if coc_client_handling:
                await self.close_coc_client_session()

    async def process_table(self, coc_client_handling: bool = True) -> None:
        """
        Processes the clan table in the database.

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

        if self.scrape_enabled:
            # Abandon scrape of clan if the clan data already exists.
            # Loop through the clan tags in reverse order so that pop() will not 
            # affect the order of the tags.
            for i in reversed(range(len(self.clans))):
                should_abandon_scrape = self.abandon_scrape_if_entity_exists and self.__does_clan_data_exist(self.clans[i])
                if should_abandon_scrape:
                    LOGGER.info(f'Abandoning member scrape for the clan {self.clans[i]} because the clan data already exists.')
                    self.clans.pop(i)

            LOGGER.info(f'Clan table {self.table_name} is updating.')
            async for clan in self.coc_client.get_clans(self.clans):
                try:
                    LOGGER.debug(f'Updating table with clan {clan} data.')
                    self.__update_table(clan)
                    await self.__scrape_members_data_if_needed(clan)
                except Exception as ex:
                    LOGGER.error(f'Unable to update table with clan {clan} data.')
                    LOGGER.error(str(ex))
        else:
            LOGGER.info(f'Clan table {self.table_name} is not updated because ClanSettings.ScrapeEnabled is {self.scrape_enabled}.')

        if coc_client_handling:
            await self.close_coc_client_session()
