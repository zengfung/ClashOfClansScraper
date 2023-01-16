import logging
import coc
import datetime

from collections.abc import Iterator
from scraper import CONFIG
from scraper.player_troops import PlayerTroopsTableHandler
from scraper.storage import TableStorageHandler
from scraper.coc_client import CocClientHandler
from scraper.utils import try_get_attr
from azure.data.tables import TableEntity

LOGGER = logging.getLogger(__name__)

class PlayerTableHandler(CocClientHandler):
    """
    The table contains a player's current progress in the game. Currently, 
    this means that only in-game achievements and troop levels are being 
    logged, other data such as building levels, attack/defense logs are not 
    collected as they're not scrape-able via Clash of Clans API.

    Attributes
    ----------
    table_name : str
        The name of the table in Azure Table Storage.
    players : list[str]
        A list of player tags whose data needs to be scraped.
    scrape_enabled : bool
        Whether player data should be scraped or not.
    abandon_scrape_if_entity_exists : bool
        Determines if the scrape should be abandoned if the entity exists in
        the table.

    Methods
    -------
    scrape_clan_members(member_tags: Generator[str,None,None], coc_client_handling: bool = True) -> None:
        Scrapes the clan members and updates the table.
    scrape_location_players(players: list[coc.players.RankedPlayer], coc_client_handling: bool = True) -> None:
        Scrapes the list of players from a location and updates the table.
    process_table(coc_client_handling: bool = True) -> None
        Updates the player table.
    """

    configs = CONFIG['PlayerSettings']
    table_name = configs['TableName']
    players = configs['Players']
    scrape_enabled = configs['ScrapeEnabled']
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
        self.troop_handler = PlayerTroopsTableHandler(**kwargs)
        self.table_handler = TableStorageHandler(table_name=self.table_name, **kwargs)

    def __convert_data_to_entity_list(self, data: coc.abc.BasePlayer) -> Iterator[TableEntity]:
        """
        Converts the data to a list of entities.

        Parameters
        ----------
        data : coc.abc.BasePlayer
            The player data to add to the entity.

        Yields
        ------
        TableEntity
            The entity corresponding to the input player and their base
            information.
        """

        entity = TableEntity()

        # Mandatory keys
        entity['PartitionKey'] = self.__get_partition_key(player=try_get_attr(data, 'tag'))
        entity['RowKey'] = self.__get_row_key()

        # Identity keys
        entity['SeasonId'] = datetime.datetime.now().strftime('%Y-%m')
        entity['Tag'] = try_get_attr(data, 'tag')
        entity['Name'] = try_get_attr(data, 'name')

        # Clan-level details
        entity['Clan'] = try_get_attr(data.clan, 'tag') if try_get_attr(data, 'clan') is not None else None
        entity['Role'] = try_get_attr(data, 'role')
        entity['ClanRank'] = try_get_attr(data, 'clan_rank')
        entity['ClanPreviousRank'] = try_get_attr(data, 'clan_previous_rank')
        entity['Donations'] = try_get_attr(data, 'donations')
        entity['Received'] = try_get_attr(data, 'received')

        # Player-level details
        entity['ExpLevel'] = try_get_attr(data, 'exp_level')
        entity['LeagueId'] = try_get_attr(data, 'league_id')
        entity['Trophies'] = try_get_attr(data, 'trophies')
        entity['VersusTrophies'] = try_get_attr(data, 'versus_trophies')
        entity['ClanCapitalContributions'] = try_get_attr(data, 'clan_capital_contributions')
        entity['AttackWins'] = try_get_attr(data, 'attack_wins')
        entity['DefenseWins'] = try_get_attr(data, 'defense_wins')
        entity['VersusAttackWins'] = try_get_attr(data, 'versus_attack_wins')
        entity['BestTrophies'] = try_get_attr(data, 'best_trophies')
        entity['BestVersusTrophies'] = try_get_attr(data, 'best_versus_trophies')
        entity['WarStars'] = try_get_attr(data, 'war_stars')
        entity['WarOptedIn'] = try_get_attr(data, 'war_opted_in')
        entity['TownHall'] = try_get_attr(data, 'town_hall')
        entity['TownHallWeapon'] = try_get_attr(data, 'town_hall_weapon')
        entity['BuilderHall'] = try_get_attr(data, 'builder_hall')

        yield entity

    async def __get_data(self, player: str) -> Iterator[TableEntity]:
        """
        Gets the data from the API and converts to an enumerator of entities
        to be written to the table.

        Parameters
        ----------
        player : str
            The player tag to get data for.
        
        Returns
        -------
        collections.abc.Iterator[azure.core.tables.TableEntity]
            All the entities corresponding to the input player.
        """

        data = await self.coc_client.get_player(player_tag=player)
        self.troop_handler.process_table(data)

        return self.__convert_data_to_entity_list(data)

    def __get_partition_key(self, player: str) -> str:
        """
        Gets the partition key to use for the table.

        Parameters
        ----------
        player : str
            The player tag to get the partition key for.

        Returns
        -------
        str
            The partition key to use for the table.
        """

        return player.lstrip("#")


    def __get_row_key(self) -> str:
        """
        Gets the row key for the player.

        Returns
        -------
        str
            The row key for the player in the current month.
        """

        return datetime.datetime.now().strftime('%Y-%m-%d')

    def __does_player_data_exist(self, player: str) -> bool:
        """
        Checks if the player data exists in the table.

        Parameters
        ----------
        player : str
            The player tag to check if data exists for.
        
        Returns
        -------
        bool
            True if the player data exists in the table, False otherwise.
        """

        LOGGER.debug(f'Checking if {player} exists in table {self.table_name}.')
        
        partition_key = self.__get_partition_key(player=player)
        row_key = self.__get_row_key()

        entity = self.table_handler.try_get_entity(partition_key, row_key, select='PartitionKey', retries_remaining=self.table_handler.retry_entity_extraction_count)
        return entity is not None

    async def __update_table(self, player: str) -> None:
        """
        Updates the table with the input player's data.

        Parameters
        ----------
        player : str
            The player tag to update the table with.

        Returns
        -------
        None
        """

        should_abandon_scrape = self.abandon_scrape_if_entity_exists and self.__does_player_data_exist(player)
        if should_abandon_scrape:
            LOGGER.info(f'Abandoning scrape for {player} because it already exists.')
            return None

        entities = await self.__get_data(player)
        self.table_handler.write_data_to_table(entities=entities)

    async def scrape_location_players(self, players: Iterator[coc.players.RankedPlayer], coc_client_handling: bool = True) -> None:
        """
        Scrapes the players from the input location.

        Parameters
        ----------
        players : List[coc.players.RankedPlayer]
            The players to scrape.
        coc_client_handling : bool, optional
            (Default: True) Whether or not to handle the coc client session
            automatically.

        Returns
        -------
        None
        """

        if coc_client_handling:
            await self.start_coc_client_session()
        
        LOGGER.debug(f'Player table {self.table_name} is updating.')
        for player in players:
            try:
                LOGGER.debug(f'Updating table with {try_get_attr(player, "tag")}\'s data.')
                await self.__update_table(player=try_get_attr(player, 'tag'))
            except Exception as ex:
                LOGGER.error(f'Error updating table with {try_get_attr(player, "tag")}\'s data.')
                LOGGER.error(ex)

        if coc_client_handling:
            await self.close_coc_client_session()

    async def scrape_clan_members(self, member_tags: Iterator[str], coc_client_handling: bool = True) -> None:
        """
        Scrapes the clan members and updates the table.

        Parameters
        ----------
        member_tags : Generator[str,None,None]
            The clan members' player tags for data scraping.
        coc_client_handling : bool, optional
            (Default: True) Whether or not to handle the coc client session
            automatically.

        Returns
        -------
        None
        """

        if coc_client_handling:
            await self.start_coc_client_session()

        LOGGER.debug(f'Player table {self.table_name} is updating.')
        for member_tag in member_tags:
            try:
                LOGGER.debug(f'Updating table with player {member_tag} data.')
                await self.__update_table(player=member_tag)
            except Exception as ex:
                LOGGER.error(f'Unable to update table with {member_tag} data.')
                LOGGER.error(str(ex))

        if coc_client_handling:
            await self.close_coc_client_session()

    async def process_table(self, coc_client_handling: bool = True) -> None:
        """
        Updates the player table.

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
            LOGGER.debug(f'Player table {self.table_name} is updating.')
            for player in self.players:
                try:
                    LOGGER.debug(f'Updating table with player {player} data.')
                    await self.__update_table(player)
                except Exception as ex:
                    LOGGER.error(f'Unable to update table with {player} data.')
                    LOGGER.error(str(ex))
        else:
            LOGGER.info(f'Player table {self.table_name} is not updated because PlayerSettings.ScrapeEnabled is {self.scrape_enabled}.')

        if coc_client_handling:
                await self.close_coc_client_session()