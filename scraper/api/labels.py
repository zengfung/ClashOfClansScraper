from scraper.api import ClashOfClansAPI
from typing import Dict

class Labels(ClashOfClansAPI):
    def __init__(self, token:str, timeout:int = 30) -> None:
        ClashOfClansAPI.__init__(self, token, timeout)

    def get_players_labels(self,
                           limit:int = 10,
                           after:str = None,
                           before:str = None) -> Dict:
        uri = f'/labels/players'
        params = {
            'limit': limit,
            'after': after,
            'before': before
        }
        return self.get(uri, params=params)

    def get_clans_labels(self,
                        limit:int = 10,
                        after:str = None,
                        before:str = None) -> Dict:
        uri = f'/labels/clans'
        params = {
            'limit': limit,
            'after': after,
            'before': before
        }
        return self.get(uri, params=params)