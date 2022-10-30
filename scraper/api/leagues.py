from scraper.api import ClashOfClansAPI
from typing import Dict

class Leagues(ClashOfClansAPI):
    def __init__(self, token:str, timeout:int = 30) -> None:
        ClashOfClansAPI.__init__(self, token, timeout)

    def get_leagues(self,
                    limit:int = 10,
                    after:str = None,
                    before:str = None) -> Dict:
        uri = f'/leagues'
        params = {
            'limit': limit,
            'after': after,
            'before': before
        }
        return self.get(uri, params=params)

    def get_league_season_ranking(self,
                                  league_id:str,
                                  season_id:str,
                                  limit:int = 10,
                                  after:str = None,
                                  before:str = None) -> Dict:
        uri = f'/leagues/{league_id}/seasons/{season_id}'
        params = {
            'leagueId': league_id,
            'seasonId': season_id,
            'limit': limit,
            'after': after,
            'before': before
        }
        return self.get(uri, params=params)

    def get_league_info(self, league_id:str) -> Dict:
        uri = f'/leagues/{league_id}'
        params = {
            'leagueId': league_id
        }
        return self.get(uri, params=params)

    def get_league_seasons(self,
                           league_id:str,
                           limit:int = 10,
                           after:str = None,
                           before:str = None) -> Dict:
        uri = f'/leagues/{league_id}/seasons'
        params = {
            'leagueId': league_id,
            'limit': limit,
            'after': after,
            'before': before
        }
        return self.get(uri, params=params)

    def get_warleague_info(self,league_id:str) -> Dict:
        uri = f'/warleagues/{league_id}'
        params = {
            'leagueId': league_id
        }
        return self.get(uri, params=params)

    def get_warleagues(self,
                       limit:int = 10,
                       after:str = None,
                       before:str = None) -> Dict:
        uri = f'/warleagues'
        params = {
            'limit': limit,
            'after': after,
            'before': before
        }
        return self.get(uri, params=params)