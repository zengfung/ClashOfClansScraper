import yaml
from pathlib import Path

from .gold_pass import update_goldpass_season_table
from .players import update_players_table
from .in_game import create_dataframe_for_ingame_data

CURRENT_DIR = Path(__file__).parent
CONFIG_FILE = 'config.yaml'

with open(f'{CURRENT_DIR}/{CONFIG_FILE}', 'r') as f:
    CONFIG = yaml.load(f, Loader=yaml.FullLoader)