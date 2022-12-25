import logging

from typing import Union
from typing import Optional
from coc.abc import BasePlayer
from coc.abc import BaseClan
from coc.abc import DataContainer
from coc import Location

LOGGER = logging.getLogger(__name__)

def try_get_attr( 
        data: Union[BasePlayer,BaseClan,DataContainer,Location], 
        attr: str, 
        index: int = None,
        default: Optional[Union[float,int,str]] = None) -> Union[float,int,str]:
    """
    Returns the value of the given attribute for the given data if the
    attribute exists. Otherwise, returns None.

    Parameters
    ----------
    data : coc.abc.DataContainer
        The data to get the attribute from.
    attr : str
        The attribute to get.
    index : int, optional
        The index of the attribute to get if the attribute is a list.

    Returns
    -------
    Union[float,int,str]
        The value of the attribute if it exists. Otherwise, returns None.
    """

    out = getattr(data, attr, default)

    if out is None:
        return default

    if isinstance(out, list) and len(out) == 0:
        return default

    if index is not None:
        try:
            return out[index]
        except IndexError as ex:
            LOGGER.error(f'IndexError obtained at {attr}.')
            LOGGER.error(str(ex))
            return default

    return out