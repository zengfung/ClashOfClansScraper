import logging
import time
import coc

from scraper import CONFIG

logging.getLogger('coc').setLevel(logging.WARNING)
LOGGER = logging.getLogger(__name__)

class CocClientHandler(object):
    """
    The storage handler object is used to connect to the Clash of Clans API
    client and a table in Azure Table Storage. Connection retries and data 
    modifications on the corresponding table via inserting, upserting, and 
    deleting entities are handled by this class.

    Attributes
    ----------
    coc_client : coc.Client
        The Clash of Clans API client object.
    
    Methods
    -------
    restart_coc_client_session() -> None
        Restarts the Clash of Clans API client session.
    close_coc_client_session() -> None
        Closes the Clash of Clans API client session.
    start_coc_client_session() -> None
        Starts the Clash of Clans API client session.
    """

    restart_sleep_time = CONFIG['CocClient']['RestartSleepTime']

    def __init__(
            self,
            coc_email: str,
            coc_password: str,
            coc_client: coc.Client = None) -> None:
        """
        Parameters
        ----------
        coc_email : str
            The email address of the Clash of Clans API account.
        coc_password : str
            The password of the Clash of Clans API account.
        coc_client : coc.Client
            (Default: None) The Clash of Clans API client object.
        """

        # Clash of Clans API Client
        self.__coc_email = coc_email
        self.__coc_password = coc_password
        # Need to manually start coc_client session
        self.coc_client = coc_client  

    async def restart_coc_client_session(self) -> None:
        """
        Restarts the Clash of Clans API client session.

        Returns
        -------
        None
        """

        LOGGER.debug(f'Restarting Clash of Clans API client session. Program will sleep for {self.restart_sleep_time} seconds after closing the session.')
        await self.close_coc_client_session()
        time.sleep(self.restart_sleep_time)
        await self.start_coc_client_session()

    async def start_coc_client_session(self) -> None:
        """
        Starts a Clash of Clans API client session.

        Returns
        -------
        None
        """

        if self.coc_client is not None:
            LOGGER.warning('Clash of Clans API client session already running.')
            return

        try:
            LOGGER.debug('Starting Clash of Clans API client session.')
            self.coc_client = coc.Client(load_game_data=coc.LoadGameData(default=True))
            await self.coc_client.login(email=self.__coc_email, password=self.__coc_password)
        except Exception as ex:
            LOGGER.error('Failed to start Clash of Clans API client session.')
            LOGGER.error(str(ex))

    async def close_coc_client_session(self) -> None:
        """
        Closes the Clash of Clans API client session.

        Returns
        -------
        None
        """

        if self.coc_client is None:
            LOGGER.warning('Clash of Clans API client session already closed.')
            return

        LOGGER.debug('Closing Clash of Clans API client session.')
        await self.coc_client.close()
        self.coc_client = None