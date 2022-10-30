from scraper.api import ClashOfClansAPI
from typing import Dict

class Clans(ClashOfClansAPI):
    def __init__(self, clan_tag:str, token:str, timeout:int = 30) -> None:
        self.clan_tag = clan_tag
        ClashOfClansAPI.__init__(self, token, timeout)

    def get_current_warleague_group(self) -> Dict:
        uri = f'/clans/{self.clan_tag}/currentwar/leaguegroup'
        params = {'clanTag': self.clan_tag}
        return self.get(uri, params=params)

    def get_warleague_war(self, war_tag:str) -> Dict:
        uri = f'/clanwarleagues/wars/{war_tag}'
        return self.get(uri)


    def get_warlog(self,
                   limit:int = 10,
                   after:str = None,
                   before:str = None) -> Dict:
        uri = f'/clans/{self.clan_tag}/warlog'
        params = {
            'clanTag': self.clan_tag,
            'limit': limit,
            'after': after,
            'before': before
        }
        return self.get(uri, params=params)

    """
    Search for clans with specific criteria
    params: {
      "name": 'SomeClanName',
      "warFrequency": ['always', 'moreThanOncePerWeek','oncePerWeek','lessThenOncePerWeek','never','Unknown'],
      "locationId": 1,
      "minMembers": 20,
      "minClanPoints": 1200,
      "minClanLevel": 1-10,
      "limit": 5,
      "after": 2,
      "before": 100
    }
    """
    def search_clans(self,
                     name:str = None,
                     war_frequency:str = None,
                     location_id:int = None,
                     min_members:int = None,
                     max_members:int = None,
                     min_clan_points:int = None,
                     min_clan_level:int = None,
                     limit:int = 10,
                     after:str = None,
                     before:str = None,
                     label_ids:str = None):
        uri = f'/clans'
        params = {
            'name': name,
            'warFrequency': war_frequency,
            'locationId': location_id,
            'minMembers': min_members,
            'maxMembers': max_members,
            'minClanPoints': min_clan_points,
            'minClanLevel': min_clan_level,
            'limit': limit,
            'after': after,
            'before': before,
            'labelIds': label_ids
        }
        return self.get(uri, params=params)

    def get_current_war(self):
        uri = f'/clans/{self.clan_tag}/currentwar'
        return self.get(uri)

    def get_clan_info(self):
        uri = f'/clans/{self.clan_tag}'
        params = {'clanTag': self.clan_tag}
        return self.get(uri, params=params)

    def get_clan_members(self,
                         limit:int = 10,
                         after:str = None,
                         before:str = None):
        uri = f'/clans/{self.clan_tag}/members'
        params = {
            'clanTag': self.clan_tag,
            'limit': limit,
            'after': after,
            'before': before
        }
        return self.get(uri, params=params)
