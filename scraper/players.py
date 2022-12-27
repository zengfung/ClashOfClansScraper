import logging
import coc
import datetime

from collections.abc import Iterator
from scraper import CONFIG
from scraper.storage import StorageHandler
from scraper.utils import try_get_attr
from azure.data.tables import TableEntity

LOGGER = logging.getLogger(__name__)

class PlayerTableHandler(StorageHandler):
    """
    The table contains a player's current progress in the game. Currently, 
    this means that only in-game achievements and troop levels are being 
    logged, other data such as building levels, attack/defense logs are not 
    collected as they're not scrape-able via Clash of Clans API.

    Attributes
    ----------
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
    players = configs['Players']
    scrape_enabled = configs['ScrapeEnabled']
    abandon_scrape_if_entity_exists = configs['AbandonScrapeIfEntityExists']

    def __init__(self, **kwargs) -> None:
        """
        Parameters
        ----------
        **kwargs
            Keyword arguments to pass to the StorageHandler class.
        """

        super().__init__(table_name=self.configs['TableName'], **kwargs)

    def __is_super_troop_active(self, troop: coc.abc.DataContainer, data: coc.abc.BasePlayer) -> bool:
        """
        Checks if a super troop is active for the player.
        
        Parameters
        ----------
        troop : coc.abc.DataContainer
            The troop to check.
        data : coc.abc.BasePlayer
            The player data to check.

        Returns
        -------
        bool
            Whether the super troop is active or not.
        """

        return troop in data.home_troops

    def __add_base_details_to_entity(self, data: coc.abc.BasePlayer) -> TableEntity:
        """
        Adds the base details to the entity.

        Parameters
        ----------
        data : coc.abc.BasePlayer
            The player data to add to the entity.

        Returns
        -------
        TableEntity
            The entity corresponding to the input player and their base
            information.
        """

        entity = TableEntity()

        # Mandatory keys
        # PartitionKey to be defined as '{PlayerTag}-{TroopId}' when extracting troop details
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

        return entity

    def __add_troop_details_to_entity(self, base_entity: TableEntity, data: coc.abc.BasePlayer) -> Iterator[TableEntity]:
        """
        Creates a new azure.data.tables.TableEntity object for each troop
        based on the player's base details and adds the troop details to the
        entity.

        Parameters
        ----------
        base_entity : azure.core.tables.TableEntity
            The entity containing the base details to add the troop details to.
        data : coc.abc.BasePlayer
            The player data to add to the entity.
        
        Yields
        ------
        azure.core.tables.TableEntity
            The entity corresponding to the input player and their troop 
            information.
        """

        inactive_super_troops = list(set(data.super_troops) - set(data.home_troops))
        troop_list = data.heroes + data.hero_pets + data.spells + data.troops + data.builder_troops + inactive_super_troops
        
        for troop in troop_list:
            # Skip troop if troop object is None or its Id and Level is None
            if (troop is None or \
                try_get_attr(troop, 'id') is None or \
                try_get_attr(troop, 'level') is None):
                LOGGER.debug(f'Skipping {troop} as it is either (1) None, (2) has None id, or (3) has None level.')
                continue
            
            entity = base_entity.copy()

            # PartitionKey to be defined as '{PlayerTag}-{TroopId}'
            LOGGER.debug(f'Adding {troop} to entity.')
            entity['PartitionKey'] = f"{try_get_attr(data, 'tag').lstrip('#')}-{try_get_attr(troop, 'id')}"

            # Get troop details
            entity['TroopId'] = try_get_attr(troop, 'id')
            entity['TroopLevel'] = try_get_attr(troop, 'level')
            entity['TroopVillage'] = try_get_attr(troop, 'village')
            entity['TroopTownhallMaxLevel'] = troop.get_max_level_for_townhall(data.town_hall) if hasattr(troop, 'get_max_level_for_townhall') else None
            entity['TroopIsMaxForTownhall'] = try_get_attr(data, 'is_max_for_townhall')
            entity['TroopIsActive'] = self.__is_super_troop_active(troop, data) if try_get_attr(data, 'is_super_troop') is not None else None
            
            yield entity

    def __convert_data_to_entity_list(self, data: coc.abc.BasePlayer) -> Iterator[TableEntity]:
        """
        Converts the data to a list of entities.

        Parameters
        ----------
        data : coc.abc.BasePlayer
            The player data to convert to entities.
        
        Yields
        ------
        azure.core.tables.TableEntity
            All the entities corresponding to the input player.
        """

        base_entity = self.__add_base_details_to_entity(data)
        yield from self.__add_troop_details_to_entity(base_entity, data)

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
        return self.__convert_data_to_entity_list(data)

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
        
        row_key = self.__get_row_key()

        query_filter = f"RowKey eq '{row_key}' and Tag eq '{player}'"
        results = self.try_query_entities(query_filter=query_filter, retries_remaining=self.retry_entity_extraction_count, select='PartitionKey')
        
        has_results = bool(next(results, False))
        return has_results

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
        self.write_data_to_table(entities=entities)

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
        
        LOGGER.info(f'Player table {self.table_name} is updating.')
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

        LOGGER.info(f'Player table {self.table_name} is updating.')
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
            LOGGER.info(f'Player table {self.table_name} is updating.')
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