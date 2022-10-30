from pickletools import int4
from scraper import ClashOfClansAPI
from typing import Dict

class Locations(ClashOfClansAPI):
    def __init__(self, location_id:str, token:str, timeout:int = 30) -> None:
        self.location_id = location_id
        ClashOfClansAPI.__init__(self, token, timeout)

    def list_locations(self,
                       limit:int = 10,
                       after:str = None,
                       before:str = None) -> Dict:
        uri = f'/locations'
        params = {
            'limit': limit,
            'after': after,
            'before': before
        }
        return self.get(uri, params=params)

    def get_location_info(self) -> Dict:
        uri = f'/locations/{self.location_id}'
        params = {
            'locationId': self.location_id
        }
        return self.get(uri, params=params)

    def get_player_versus_rankings(self,
                                   limit:int = 10,
                                   after:str = None,
                                   before:str = None) -> Dict:
        uri = f'/locations/{self.location_id}/rankings/players-versus'
        params = {
            'locationId': self.location_id,
            'limit': limit,
            'after': after,
            'before': before
        }
        return self.get(uri, params=params)

    def get_clan_versus_rankings(self,
                                 limit:int = 10,
                                 after:str = None,
                                 before:str = None) -> Dict:
        uri = f'/locations/{self.location_id}/rankings/clans-versus'
        params = {
            'locationId': self.location_id,
            'limit': limit,
            'after': after,
            'before': before
        }
        return self.get(uri, params=params)

    def get_player_rankings(self,
                            limit:int = 10,
                            after:str = None,
                            before:str = None) -> Dict:
        uri = f'/locations/{self.location_id}/rankings/players'
        params = {
            'locationId': self.location_id,
            'limit': limit,
            'after': after,
            'before': before
        }
        return self.get(uri, params=params)

    def get_clans_rankings(self,
                           limit:int = 10,
                           after:str = None,
                           before:str = None) -> Dict:
        uri = f'/locations/{self.location_id}/rankings/clans'
        params = {
            'locationId': self.location_id,
            'limit': limit,
            'after': after,
            'before': before
        }
        return self.get(uri, params=params)