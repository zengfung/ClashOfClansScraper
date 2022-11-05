import coc
import logging
import pandas as pd
from pathlib import Path
from typing import Union, List, Tuple

LOGGER = logging.getLogger(__name__)

def create_or_append_table_if_needed(df:pd.DataFrame, dir:str, filename:str) -> None:
    path = Path(dir + "/" + filename)

    if (path.is_file() and has_new_unique_rows(df, path)) or \
       (not path.is_file()):
        LOGGER.debug(f'Creating or appending table required.')
        create_or_append_table(df, dir, filename)
    else:
        LOGGER.debug(f'Current dataframe rows exist in existing dataframe, no addition needed.')

def create_or_append_table(df:pd.DataFrame, dir:str, filename:str) -> None:
    path = Path(dir + "/" + filename)
    if path.is_file():
        LOGGER.debug(f'File {path} exists, appending data.')
        df.to_csv(path, mode='a', index=False, header=False)
    else:
        LOGGER.debug(f'File {path} does not exist, writing data as a new file.')
        Path(dir).mkdir(parents=True, exist_ok=True)
        df.to_csv(path, mode='w', index=False, header=True)

def has_new_unique_rows(df_new:pd.DataFrame, path:Path) -> bool:
    LOGGER.debug(f'Reading existing data from {path}.')
    df_old = pd.read_csv(path)
    
    LOGGER.debug(f'Getting rows in new dataframe that is unique from old dataframe.')
    df_new.drop(df_new[df_new.isin(df_old)].index, inplace=True)
    if len(df_new) > 0:
        return True
    else:
        return False

def convert_time_to_seconds(time:Union[coc.TimeDelta,List[coc.TimeDelta]]) -> Union[int,List[int]]:
    if isinstance(time, coc.TimeDelta):
        return time.total_seconds()
    else:
        return [t.total_seconds() for t in time]