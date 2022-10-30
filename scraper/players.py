from scraper import ClashOfClansAPI

class Players(ClashOfClansAPI):
    def __init__(self, player_tag:str, token:str, timeout:int = 30) -> None:
        self.player_tag = player_tag
        ClashOfClansAPI.__init__(self, token, timeout)

    def get_player_info(self):
        uri = f'/players/{self.player_tag}'
        params = {'playerTag': self.player_tag}
        return self.get(uri, params=params)
