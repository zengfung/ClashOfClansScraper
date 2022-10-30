from scraper import ClashOfClansAPI

class GoldPass(ClashOfClansAPI):
    def __init__(self,
                 token:str,
                 timeout:int = 30) -> None:
        ClashOfClansAPI.__init__(self, token, timeout)

    def get_current_season(self):
        uri = "/goldpass/seasons/current"
        return self.get(uri)