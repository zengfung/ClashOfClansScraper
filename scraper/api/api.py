from typing import Dict
import requests

class ClashOfClansAPI:
    def __init__(self, 
                 token:str, 
                 timeout:int = 30) -> None:
        self.api_endpoint = "https://api.clashofclans.com/v1"
        self.token = token
        self.timeout = timeout
        self.headers = {
            'Accept': 'application/json',
            'Authorization': 'Bearer ' + self.token
        }

    def get(self, 
            uri:str, 
            params:Dict = None) -> Dict:
        try:
            url = self.api_endpoint + uri
            response = requests.get(url, params=params, headers=self.headers, timeout=self.timeout)
            return response.json()
        except:
            return f'Error {response.status_code}'