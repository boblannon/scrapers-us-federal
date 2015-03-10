import copy
from pupa.scrape.schemas.common import fuzzy_date, fuzzy_datetime_blank


def pupa_date(parse_properties):
    pd = copy.deepcopy(fuzzy_date)
    pd.update(parse_properties)
    return pd

def pupa_datetime_blank(parse_properties):
    pd = copy.deepcopy(fuzzy_datetime_blank)
    pd.update(parse_properties)
    return pd
