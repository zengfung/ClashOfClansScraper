import logging
import coc
import datetime

from typing import Dict
from typing import Union
from typing import Generator
from typing import List
from scraper import CONFIG
from scraper.players import PlayerTableHandler
from scraper.storage import StorageHandler
from scraper.utils import try_get_attr

LOGGER = logging.getLogger(__name__)

class ClanTableHandler(StorageHandler):
    """
    The table contains a clan's current progress in the game.

    Attributes
    ----------
    coc_client : coc.Client
        The coc client used to connect to the Clash of Clans API.
    table : str
        The name of the table to be written to.
    scrape_enabled : bool
        Whether clan data should be scraped or not.
    member_scrape_enabled : bool
        Whether clan member data should be scraped or not.
    clans : List[str]
        The list of clan tags whose data needs to be scraped.
    kwargs : Dict[str,Union[str,bool]]
        The kwargs used to initialize the StorageHandler.

    Methods
    -------
    __get_member_tags__(clan: coc.Clan) -> Generator[str,None,None]
        Returns a generator of the clan's member tags.
    __scrape_members_data__(clan: coc.Clan) -> None
        Scrapes the clan's member data by calling PlayerTableHandler.
    __scrape_members_data_if_needed__(clan: coc.Clan) -> None
        Scrapes the clan's member data if ClanSettings.MemberScrapeEnabled is True.
    __convert_data_to_entity_list__(clan: coc.Clan) -> Generator[Dict[str,Union[float,int,str]],None,None]
        Converts the clan's data to a list of entities.
    __update_table__(clan: coc.Clan) -> None
        Updates the table with the clan's data.
    process_table() -> None
        Processes the clan table in the database.
    """

    configs = CONFIG['ClanSettings']
    table = configs['TableName']
    clans = configs['Clans']
    scrape_enabled = configs['ScrapeEnabled']
    member_scrape_enabled = configs['MemberScrapeEnabled']

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

    def __get_member_tags__(self, clan: coc.Clan) -> Generator[str,None,None]:
        """
        Returns an iterator of the clan's member tags.

        Parameters
        ----------
        clan : coc.Clan
            The clan whose member tags need to be scraped.

        Returns
        -------
        Generator[str,None,None]
            An iterator of the clan's member tags.
        """

        assert isinstance(clan, coc.Clan)

        for member in try_get_attr(clan, 'members'):
            yield try_get_attr(member, 'tag')

    async def __scrape_members_data__(self, clan: coc.Clan) -> None:
        """
        Scrapes the clan's member data by calling PlayerTableHandler.
        
        Parameters
        ----------
        clan : coc.Clan
            The clan whose member data needs to be scraped.
        """

        assert isinstance(clan, coc.Clan)

        scraper = PlayerTableHandler(self.coc_client, **self.kwargs)
        member_tags = self.__get_member_tags__(clan)
        await scraper.scrape_clan_members(member_tags)

    async def __scrape_members_data_if_needed__(self, clan: coc.Clan) -> None:
        """
        Scrapes the clan's member data if ClanSettings.MemberScrapeEnabled is True.
        
        Parameters
        ----------
        clan : coc.Clan
            The clan whose member data needs to be scraped.
        """

        if self.member_scrape_enabled:
            assert isinstance(clan, coc.Clan)

            LOGGER.debug(f'Scraping members data for clan {try_get_attr(clan, "tag")}.')
            await self.__scrape_members_data__(clan)
        else:
            LOGGER.debug(f'Not scraping members data for clan {try_get_attr(clan, "tag")} because ClanSettings.MemberScrapeEnabled is {self.member_scrape_enabled}.')

    def __convert_data_to_entity_list__(self, clan: coc.Clan) -> Generator[Dict[str,Union[float,int,str]],None,None]:
        """
        Converts the given data to a list of entities to insert/upsert to
        the table.

        Parameters
        ----------
        clan : coc.Clan
            The clan whose data needs to be converted.

        Returns
        -------
        Generator[Dict[str,Union[float,int,str]],None,None]
            A generator of entities to insert/upsert to the table.
        """

        entity = dict()

        entity['PartitionKey'] = try_get_attr(clan, 'tag').lstrip('#')
        entity['RowKey'] = datetime.datetime.now().strftime('%Y-%m-%d')
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

    def __update_table__(self, clan: coc.Clan) -> None:
        """
        Updates the table with the clan's data.

        Parameters
        ----------
        clan : coc.Clan
            The clan whose data needs to be updated.
        """

        # TODO: How to prevent data scraping if data is already present in the table?
        entities = self.__convert_data_to_entity_list__(clan)
        self.__write_data_to_table__(entities=entities)

    async def scrape_location_clans(self, clans: List[coc.clans.RankedClan]) -> None:
        """
        Scrapes the given location's clans.

        Parameters
        ----------
        clan_tags : List[RankedClan]
            The list of ranked clans to scrape.
        """

        for clan in clans:
            try:
                LOGGER.debug(f'Scraping clan {try_get_attr(clan, "tag")}.')
                clan = await self.coc_client.get_clan(try_get_attr(clan, 'tag')) if not isinstance(clan, coc.Clan) else clan
                self.__update_table__(clan)
                await self.__scrape_members_data_if_needed__(clan)
            except Exception as ex:
                LOGGER.error(f'Error while scraping clan {try_get_attr(clan, "tag")}: {ex}')
                LOGGER.error(str(ex))

    async def process_table(self) -> None:
        """
        Processes the clan table in the database.
        """

        if self.scrape_enabled:
            LOGGER.info(f'Clan table {self.table} is updating.')
            async for clan in self.coc_client.get_clans(self.clans):
                try:
                    LOGGER.debug(f'Updating table with clan {clan} data.')
                    self.__update_table__(clan)
                    await self.__scrape_members_data_if_needed__(clan)
                except Exception as ex:
                    LOGGER.error(f'Unable to update table with clan {clan} data.')
                    LOGGER.error(str(ex))
        else:
            LOGGER.info(f'Clan table {self.table} is not updated because ClanSettings.ScrapeEnabled is {self.scrape_enabled}.')
