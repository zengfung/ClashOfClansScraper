import yaml
from pathlib import Path

CURRENT_DIR = Path(__file__).parent
CONFIG_FILE = 'config.yaml'

with open(f'{CURRENT_DIR}/{CONFIG_FILE}', 'r') as f:
    CONFIG = yaml.load(f, Loader=yaml.FullLoader)