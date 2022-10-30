from typing import Dict
import requests

class ClashOfClansAPI:
    """
    Parent class for calling the Clash of Clans API.
    """

    def __init__(self, 
                 token:str, 
                 timeout:int = 30) -> None:
        """
        Initialize class with API endpoint, bearer token, and API call timeout.

        """
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
        """
        Makes a call to the API endpoint.

        Arguments
        """
        try:
            url = self.api_endpoint + uri
            response = requests.get(url, params=params, headers=self.headers, timeout=self.timeout)
            return response.json()
        except:
            return f'Error {response.status_code}'