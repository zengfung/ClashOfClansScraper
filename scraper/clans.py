import logging
import coc
import datetime

from typing import Dict, Union, Generator
from scraper import CONFIG
from scraper.players import PlayerTableHandler
from scraper.storage import StorageHandler

LOGGER = logging.getLogger(__name__)

class ClanTableHandler(StorageHandler):
    """
    Handles the clan table in the database.
    """

    configs = CONFIG['ClanSettings']
    table = configs['TableName']
    clans = configs['Clans']
    scrape_enabled = configs['ScrapeEnabled']
    member_scrape_enabled = configs['MemberScrapeEnabled']

    def __init__(self, coc_client: coc.Client, **kwargs) -> None:
        super().__init__(self.table, **kwargs)
        self.coc_client = coc_client
        self.kwargs = kwargs

    def __get_member_tags__(self, clan: coc.Clan) -> Generator[str,None,None]:
        for member in clan.members:
            yield member.tag

    async def __scrape_members_data__(self, clan: coc.Clan) -> None:
        scraper = PlayerTableHandler(self.coc_client, **self.kwargs)
        member_tags = self.__get_member_tags__(clan)
        await scraper.scrape_clan_members(member_tags)

    async def __scrape_members_data_if_needed__(self, clan: coc.Clan) -> None:
        if self.member_scrape_enabled:
            LOGGER.debug(f'Scraping members data for clan {clan.tag}.')
            await self.__scrape_members_data__(clan)
        else:
            LOGGER.debug(f'Not scraping members data for clan {clan.tag} because ClanSettings.MemberScrapeEnabled is {self.member_scrape_enabled}.')

    def __convert_data_to_entity_list__(self, clan: coc.Clan) -> Generator[Dict[str,Union[float,int,str]],None,None]:
        self.entity = dict()
        self.entity['PartitionKey'] = clan.tag.lstrip('#')
        self.entity['RowKey'] = datetime.datetime.now().strftime('%Y-%m-%d')
        self.entity['Name'] = clan.name
        self.entity['Tag'] = clan.tag
        self.entity['Level'] = clan.level
        self.entity['Type'] = clan.type
        self.entity['Description'] = clan.description
        self.entity['Location'] = clan.location.id
        self.entity['Points'] = clan.points
        self.entity['VersusPoints'] = clan.versus_points
        self.entity['RequiredTrophies'] = clan.required_trophies
        self.entity['WarFrequency'] = clan.war_frequency
        self.entity['WarWinStreak'] = clan.war_win_streak
        self.entity['WarWins'] = clan.war_wins
        self.entity['WarTies'] = clan.war_ties
        self.entity['WarLosses'] = clan.war_losses
        self.entity['IsWarLogPublic'] = clan.public_war_log
        self.entity['MemberCount'] = clan.member_count

        yield self.entity

    async def __update_table__(self, clan: coc.Clan) -> None:
        entities = self.__convert_data_to_entity_list__(clan)
        self.__write_data_to_table__(entities=entities)

    async def process_table(self) -> None:
        """
        Processes the clan table in the database.
        """

        if self.scrape_enabled:
            LOGGER.info(f'Clan table {self.table} is updating.')
            async for clan in self.coc_client.get_clans(self.clans):
                try:
                    LOGGER.debug(f'Updating table with clan {clan} data.')
                    await self.__update_table__(clan)
                    await self.__scrape_members_data_if_needed__(clan)
                except Exception as ex:
                    LOGGER.error(f'Unable to update table with clan {clan} data.')
                    LOGGER.error(str(ex))
        else:
            LOGGER.info(f'Clan table {self.table} is not updated because ClanSettings.ScrapeEnabled is {self.scrape_enabled}.')
