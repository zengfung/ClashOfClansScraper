import logging
import coc
import datetime

from typing import Dict
from typing import Union
from typing import Optional
from typing import Generator
from scraper import CONFIG
from scraper.storage import StorageHandler

LOGGER = logging.getLogger(__name__)

class PlayerTableHandler(StorageHandler):
    """
    The player table is updated every 24 hours. The table contains a 
    player's current progress in the game. Currently, this means that only 
    in-game achievements and troop levels are being logged, other data such 
    as building levels, attack/defense logs are not collected as they're not 
    scrape-able via Clash of Clans API.

    Attributes
    ----------
    coc_client : coc.Client
        The client used to connect to the Clash of Clans API.
    entity : dict
        The current entity to be written to the table.
    table : str
        The name of the table to be written to.
    scrape_enabled : bool
        Whether player data should be scraped or not.
    players : list
        A list of player tags whose data needs to be scraped.

    Methods
    -------
    __try_get_attr__(data: coc.abc.BasePlayer, attr: str, index: Optional[int] = None) -> Union[float,int,str]
        Tries to get an attribute from the data object. If the attribute 
        is not found, returns None.
    __is_super_troop_active__(troop: coc.abc.DataContainer, data: coc.abc.BasePlayer) -> bool
        Checks if a super troop is active for the player.
    __add_base_details_to_entity__(data: coc.abc.BasePlayer) -> None
        Adds the base details to the entity.
    __add_troop_details_to_entity__(data: coc.abc.BasePlayer) -> None
        Adds the troop details to the entity.
    __convert_data_to_entity_list__(data: coc.abc.BasePlayer) -> Generator[Dict[str,Union[float,int,str]],None,None]
        Converts the data object to a list of entities to insert/upsert to
        the table.
    __get_data__(player: str) -> Generator[Dict[str,Union[float,int,str]],None,None]
        Returns a generator of dictionaries containing the player data to be
        inserted/upserted to the table.
    __update_table__(player: str) -> None
        Updates the table for a given player.
    process_table() -> None
        Updates the player table.
    """

    configs = CONFIG['PlayerSettings']
    table = configs['TableName']
    scrape_enabled = configs['ScrapeEnabled']
    players = configs['Players']

    def __init__(self, coc_client:coc.Client, **kwargs) -> None:
        """
        Parameters
        ----------
        coc_client : coc.Client
            The client used to connect to the Clash of Clans API.
        """

        super().__init__(self.table, **kwargs)
        self.coc_client = coc_client

    def __try_get_attr__(self, data: coc.abc.BasePlayer, attr: str, index: Optional[int] = None) -> Union[float,int,str]:
        """
        Try to get an attribute from the data object. If the attribute is not
        found, returns None.    

        Parameters
        ----------
        data : coc.abc.BasePlayer
            The data object to get the attribute from.
        attr : str
            The attribute to get.
        index : Optional[int]
            The index to get the attribute from. If None, the attribute is
            assumed to be a single value.

        Returns
        -------
        Union[float,int,str]
            The attribute value.
        """

        out = getattr(data, attr, None)
        if out is not None and index is not None:
            try:
                return out[index]
            except IndexError as ex:
                LOGGER.error(str(ex))
                return None
        return out

    def __is_super_troop_active__(self, troop: coc.abc.DataContainer, data: coc.abc.BasePlayer) -> bool:
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

        return True if troop in data.home_troops else False

    def __add_base_details_to_entity__(self, data: coc.abc.BasePlayer) -> None:
        """
        Adds the base details to the entity.

        Parameters
        ----------
        data : coc.abc.BasePlayer
            The player data to add to the entity.
        """

        # Mandatory keys
        # PartitionKey to be defined as '{PlayerTag}-{TroopId}' when extracting troop details
        self.entity['RowKey'] = datetime.datetime.now().strftime('%Y-%m-%d')

        # Identity keys
        self.entity['SeasonId'] = datetime.datetime.now().strftime('%Y-%m')
        self.entity['Tag'] = self.__try_get_attr__(data, 'tag')
        self.entity['Name'] = self.__try_get_attr__(data, 'name')

        # Clan-level details
        self.entity['Clan'] = self.__try_get_attr__(data.clan, 'tag') if self.__try_get_attr__(data, 'clan') is not None else None
        self.entity['Role'] = self.__try_get_attr__(data, 'role')
        self.entity['ClanRank'] = self.__try_get_attr__(data, 'clan_rank')
        self.entity['ClanPreviousRank'] = self.__try_get_attr__(data, 'clan_previous_rank')
        self.entity['Donations'] = self.__try_get_attr__(data, 'donations')
        self.entity['Received'] = self.__try_get_attr__(data, 'received')

        # Player-level details
        self.entity['ExpLevel'] = self.__try_get_attr__(data, 'exp_level')
        self.entity['LeagueId'] = self.__try_get_attr__(data, 'league_id')
        self.entity['Trophies'] = self.__try_get_attr__(data, 'trophies')
        self.entity['VersusTrophies'] = self.__try_get_attr__(data, 'versus_trophies')
        self.entity['ClanCapitalContributions'] = self.__try_get_attr__(data, 'clan_capital_contributions')
        self.entity['AttackWins'] = self.__try_get_attr__(data, 'attack_wins')
        self.entity['DefenseWins'] = self.__try_get_attr__(data, 'defense_wins')
        self.entity['VersusAttackWins'] = self.__try_get_attr__(data, 'versus_attack_wins')
        self.entity['BestTrophies'] = self.__try_get_attr__(data, 'best_trophies')
        self.entity['BestVersusTrophies'] = self.__try_get_attr__(data, 'best_versus_trophies')
        self.entity['WarStars'] = self.__try_get_attr__(data, 'war_stars')
        self.entity['WarOptedIn'] = self.__try_get_attr__(data, 'war_opted_in')
        self.entity['TownHall'] = self.__try_get_attr__(data, 'town_hall')
        self.entity['TownHallWeapon'] = self.__try_get_attr__(data, 'town_hall_weapon')
        self.entity['BuilderHall'] = self.__try_get_attr__(data, 'builder_hall')

    def __add_troop_details_to_entity(self, data: coc.abc.BasePlayer) -> Generator[Dict[str,Union[float,int,str]],None,None]:
        """
        Adds the troop details to the entity.

        Parameters
        ----------
        data : coc.abc.BasePlayer
            The player data to add to the entity.
        
        Yields
        ------
        Dict[str,Union[float,int,str]]
            The entity corresponding to the input player and their troop 
            information.
        """

        inactive_super_troops = list(set(data.super_troops) - set(data.home_troops))
        troop_list = data.heroes + data.hero_pets + data.spells + data.troops + data.builder_troops + inactive_super_troops
        
        for troop in troop_list:
            # Skip troop if troop object is None or its Id and Level is None
            if (troop is None or \
                self.__try_get_attr__(troop, 'id') is None or \
                self.__try_get_attr__(troop, 'level') is None):
                LOGGER.debug(f'Skipping {troop} as it is either (1) None, (2) has None id, or (3) has None level.')
                continue
            
            # PartitionKey to be defined as '{PlayerTag}-{TroopId}'
            LOGGER.warning(f'Adding {troop} to entity.')
            self.entity['PartitionKey'] = f"{self.__try_get_attr__(data, 'tag').lstrip('#')}-{self.__try_get_attr__(troop, 'id')}"

            # Get troop details
            self.entity['TroopId'] = self.__try_get_attr__(troop, 'id')
            self.entity['TroopLevel'] = self.__try_get_attr__(troop, 'level')
            self.entity['TroopVillage'] = self.__try_get_attr__(troop, 'village')
            self.entity['TroopTownhallMaxLevel'] = troop.get_max_level_for_townhall(data.town_hall) if hasattr(troop, 'get_max_level_for_townhall') else None
            self.entity['TroopIsMaxForTownhall'] = self.__try_get_attr__(data, 'is_max_for_townhall')
            self.entity['TroopIsActive'] = self.__is_super_troop_active__(troop, data) if self.__try_get_attr__(data, 'is_super_troop') is not None else None
            yield self.entity

    def __convert_data_to_entity_list__(self, data: coc.abc.BasePlayer) -> Generator[Dict[str,Union[float,int,str]],None,None]:
        """
        Converts the data to a list of entities.

        Parameters
        ----------
        data : coc.abc.BasePlayer
            The player data to convert to entities.
        
        Yields
        ------
        Dict[str,Union[float,int,str]]
            All the entities corresponding to the input player.
        """

        self.entity = dict()
        self.__add_base_details_to_entity__(data)
        yield from self.__add_troop_details_to_entity(data)

    async def __get_data__(self, player: str) -> Generator[Dict[str,Union[float,int,str]],None,None]:
        """
        Gets the data from the API and converts to an enumerator of entities
        to be written to the table.

        Parameters
        ----------
        player : str
            The player tag to get data for.
        
        Yields
        ------
        Dict[str,Union[float,int,str]]
            All the entities corresponding to the input player.
        """

        data = await self.coc_client.get_player(player_tag=player)
        return self.__convert_data_to_entity_list__(data)

    async def __update_table__(self, player: str) -> None:
        """
        Updates the table with the input player's data.

        Parameters
        ----------
        player : str
            The player tag to update the table with.
        """

        entities = await self.__get_data__(player)
        self.__write_data_to_table__(entities=entities)

    async def scrape_clan_members(self, member_tags: Generator[str,None,None]) -> None:
        """
        Scrapes the clan members and updates the table.

        Parameters
        ----------
        clan_tag : str
            The clan tag to scrape the members from.
        """

        LOGGER.info(f'Player table {self.table} is updating.')
        for member_tag in member_tags:
            try:
                LOGGER.debug(f'Updating table with player {member_tag} data.')
                await self.__update_table__(player=member_tag)
            except Exception as ex:
                LOGGER.error(f'Unable to update table with {member_tag} data.')
                LOGGER.error(str(ex))

    async def process_table(self) -> None:
        """
        Updates the player table.
        """

        if self.scrape_enabled:
            LOGGER.info(f'Player table {self.table} is updating.')
            for player in self.players:
                try:
                    LOGGER.debug(f'Updating table with player {player} data.')
                    await self.__update_table__(player)
                except Exception as ex:
                    LOGGER.error(f'Unable to update table with {player} data.')
                    LOGGER.error(str(ex))
        else:
            LOGGER.info(f'Player table {self.table} is not updated because PlayerSettings.ScrapeEnabled is {self.scrape_enabled}.')