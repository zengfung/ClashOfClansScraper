import logging
import coc
import datetime

from typing import List
from scraper import CONFIG
from scraper.storage import StorageHandler

LOGGER = logging.getLogger(__name__)

class PlayerTableHandler(StorageHandler):

    configs = CONFIG['PlayerSettings']
    table = configs['TableName']
    scrape_enabled = configs['ScrapeEnabled']
    players = configs['Players']

    def __init__(self, coc_client:coc.Client, **kwargs) -> None:
        super().__init__(self.table, **kwargs)
        self.coc_client = coc_client

    def __try_get_attr__(self, data, attr, index=None):
        out = getattr(data, attr, None)
        if out is not None and index is not None:
            try:
                return out[index]
            except IndexError as ex:
                LOGGER.error(str(ex))
                return None
        return out

    def __is_super_troop_active__(self, troop:coc, data) -> bool:
        return True if troop in data.home_troops else False

    def __add_base_details_to_entity__(self, data):
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

    def __add_troop_details_to_entity(self, data):
        inactive_super_troops = list(set(data.super_troops) - set(data.home_troops))
        troop_list = data.heroes + data.hero_pets + data.spells + data.troops + data.builder_troops + inactive_super_troops
        
        for troop in troop_list:
            # Skip troop if troop object is None or its Id and Level is None
            if (troop is None or \
                self.__try_get_attr__(troop, 'id') is None or \
                self.__try_get_attr__(troop, 'level') is None):
                continue
            
            # PartitionKey to be defined as '{PlayerTag}-{TroopId}'
            self.entity['PartitionKey'] = f"{self.__try_get_attr__(data, 'tag').lstrip('#')}-{self.__try_get_attr__(troop, 'id')}"

            # Get troop details
            self.entity['TroopId'] = self.__try_get_attr__(troop, 'id')
            self.entity['TroopLevel'] = self.__try_get_attr__(troop, 'level')
            self.entity['TroopVillage'] = self.__try_get_attr__(troop, 'village')
            self.entity['TroopTownhallMaxLevel'] = troop.get_max_level_for_townhall(data.town_hall) if hasattr(troop, 'get_max_level_for_townhall') else None
            self.entity['TroopIsMaxForTownhall'] = self.__try_get_attr__(data, 'is_max_for_townhall')
            self.entity['TroopIsActive'] = self.__is_super_troop_active__(troop, data) if self.__try_get_attr__(data, 'is_super_troop') is not None else None
            yield self.entity

    def __convert_data_to_entity_list__(self, data):
        self.entity = dict()
        self.__add_base_details_to_entity__(data)
        yield from self.__add_troop_details_to_entity(data)

    async def __get_data__(self, player:str):
        data = await self.coc_client.get_player(player_tag=player)
        return self.__convert_data_to_entity_list__(data)

    async def __update_table__(self, player) -> None:
        entities = await self.__get_data__(player)
        self.__write_data_to_table__(entities=entities)

    async def process_table(self) -> None:
        if self.scrape_enabled:
            LOGGER.info(f'Player table {self.table} is updating.')
            for player in self.players:
                LOGGER.debug(f'Updating table with player {player} data.')
                await self.__update_table__(player)
        else:
            LOGGER.info(f'Player table {self.table} is not updated because PlayerSettings.ScrapeEnabled is {self.scrape_enabled}.')