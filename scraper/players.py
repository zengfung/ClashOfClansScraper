import logging
import coc
import pandas as pd
import scraper
import scraper.common
from datetime import datetime

FILENAME = "players.csv"
LOGGER = logging.getLogger(__name__)

async def update_players_table(client:coc.Client, dir:str) -> None:
    LOGGER.debug('Setup Player data scrape.')
    scrape_date = datetime.now().strftime('%Y-%m-%d')
    tags = scraper.CONFIG['players']
    
    LOGGER.debug('Scraping Player data.')
    async for player in client.get_players(tags):
        print(player.achievement_cls)
        print(player.heroes)
        print(player.hero_pets)
        print(player.label_cls)
        print(player.spells)
        print(player.home_troops)
        print(player.super_troops)
        print(player.attack_wins)
        print(player.defense_wins)
        print(player.best_trophies)
        print(player.war_stars)
        print(player.town_hall)
        print(player.town_hall_weapon)
