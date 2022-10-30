import logging
import pandas as pd
from pathlib import Path

LOGGER = logging.getLogger(__name__)

def create_or_append_table(df:pd.DataFrame, dir:str, filename:str):
    path = Path(dir + "/" + filename)
    if path.is_file():
        LOGGER.debug(f'File {path} exists, appending data...')
        df.to_csv(path, mode='a', index=False, header=False)
    else:
        LOGGER.debug(f'Does {path} does not exist, writing data as a new file...')
        df.to_csv(path, mode='w', index=False, header=True)