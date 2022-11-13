import logging
import coc
import pandas as pd
import scraper
import scraper.common
from datetime import datetime
from typing import List

PLAYER_FILENAME = "players.csv"
TROOP_FILENAME = 'players_troop.csv'
SPELL_FILENAME = 'players_spell.csv'
HERO_FILENAME = 'players_hero.csv'
HEROPET_FILENAME = 'players_heropet.csv'
LOGGER = logging.getLogger(__name__)

class PlayerData:
    def __init__(self, player:coc, scrape_time:datetime) -> None:
        self.player = player
        self.scrape_time = scrape_time

    def collect_all_data(self) -> None:
        LOGGER.debug(f'Collecting {self.player.name} basic information.')
        self.base_details = self.collect_base_details()

        LOGGER.debug(f'Collecting {self.player.name} troop information.')
        active_troop_list = self.player.troops
        inactive_troop_list = list(set(self.player.super_troops) - set(self.player.home_troops))
        troop_list = active_troop_list + inactive_troop_list
        LOGGER.debug(f'{self.player.name} has {len(active_troop_list)} active troops and {len(inactive_troop_list)} inactive super troops.')
        self.troop_data = self.collect_item_data(troop_list)

        LOGGER.debug(f'Collecting {self.player.name} spell information.')
        self.spell_data = self.collect_item_data(self.player.spells)

        LOGGER.debug(f'Collecting {self.player.name} hero information.')
        self.hero_data = self.collect_item_data(self.player.heroes)

        LOGGER.debug(f'Collecting {self.player.name} hero pet information.')
        self.heropet_data = self.collect_item_data(self.player.hero_pets)

    def collect_base_details(self) -> pd.DataFrame:
        data = {
            'date': self.scrape_time,
            'tag': self.player.tag,
            'name': self.player.name,
            # Clan-level details
            'clan': self.player.clan.tag if self.player.clan is not None else None,
            'role': str(self.player.role) if self.player.clan is not None else None,
            'clan_rank': self.player.clan_rank if self.player.clan is not None else None,
            'clan_previous_rank': self.player.clan_previous_rank if self.player.clan is not None else None,
            'donations': self.player.donations if self.player.clan is not None else None,
            'received': self.player.received if self.player.clan is not None else None,
            # Player-level details
            'exp_level': self.player.exp_level,
            'league_id': self.player.league.id,
            'trophies': self.player.trophies,
            'versus_trophies': self.player.versus_trophies,
            'clan_capital_contributions': self.player.clan_capital_contributions,
            'attack_wins': self.player.attack_wins,
            'defense_wins': self.player.defense_wins,
            'versus_attack_wins': self.player.versus_attack_wins,
            'best_trophies': self.player.best_trophies,
            'best_versus_trophies': self.player.best_versus_trophies,
            'war_stars': self.player.war_stars,
            'war_opted_in': self.player.war_opted_in,
            'town_hall': self.player.town_hall,
            'town_hall_weapon': self.player.town_hall_weapon,
            'builder_hall': self.player.builder_hall,
        }

        df = pd.DataFrame(data=data, index=[0])
        return df

    def collect_troop_data(self) -> pd.DataFrame:
        active_troop_list = self.player.troops
        inactive_troop_list = list(set(self.player.super_troops) - set(self.player.home_troops))
        troop_list = active_troop_list + inactive_troop_list
        data = {
            'date': self.scrape_time,
            'tag': self.player.tag,
            # Troop IDs and personal info
            'id': [],
            'level': [],
            'townhall_max_level': [],
            'village': [],
            'is_max_for_townhall': [],
            'is_active': []
        }

        for troop in troop_list:
            data['id'] += [troop.id]
            data['level'] += [troop.level]
            data['townhall_max_level'] += [troop.get_max_level_for_townhall(self.player.town_hall)]
            data['village'] += [troop.village]
            data['is_max_for_townhall'] += [item.is_max_for_townhall]
            data['is_active'] += [self.is_super_troop_active(troop) if troop.is_super_troop else None]

        df = pd.DataFrame(data=data)
        return df

    def collect_item_data(self, item_list:List) -> pd.DataFrame:
        data = {
            'date': self.scrape_time,
            'tag': self.player.tag,
            # Item IDs and personal info
            'id': [],
            'level': [],
            'townhall_max_level': [],
            'village': [],
            'is_max_for_townhall': [],
            'is_active': []
        }

        for item in item_list:
            data['id'] += [item.id]
            data['level'] += [item.level]
            data['townhall_max_level'] += [item.get_max_level_for_townhall(self.player.town_hall) if hasattr(item, 'get_max_level_for_townhall') else None]
            data['village'] += [item.village]
            data['is_max_for_townhall'] += [item.is_max_for_townhall  if hasattr(item, 'is_max_for_townhall') else None]
            data['is_active'] += [self.is_super_troop_active(item) if (hasattr(item, 'is_super_troop') and item.is_super_troop) else None]

        df = pd.DataFrame(data=data)
        return df

    def is_super_troop_active(self, troop:coc) -> bool:
        return True if troop in self.player.home_troops else False

    def update_tables(self, dir:str) -> None:
        data_file_list = [
            (self.base_details, PLAYER_FILENAME),
            (self.troop_data, TROOP_FILENAME),
            (self.spell_data, SPELL_FILENAME),
            (self.hero_data, HERO_FILENAME),
            (self.heropet_data, HEROPET_FILENAME)
        ]

        for (df, filename) in data_file_list:
            scraper.common.create_or_append_table_if_needed(df, dir, filename, ['date', 'tag'])


async def update_players_table(client:coc.Client, dir:str) -> None:
    LOGGER.debug('Setup Player data scrape.')
    scrape_date = datetime.now().strftime('%Y-%m-%d')
    tags = scraper.CONFIG['players']
    
    LOGGER.debug('Scraping Player data.')
    async for player in client.get_players(tags):
        data_obj = PlayerData(player, scrape_date)
        data_obj.collect_all_data()
        data_obj.update_tables(dir)
        
