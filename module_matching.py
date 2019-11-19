# Mikhail Ark
# PharmHub (RU)
# Nov 2019

import pandas as pd
from pickle import load
import psycopg2
from requests import post
from uuid import UUID

from libs.branch_predictor import get_pharm_group, get_manufs
from libs.cmodel.prediction import get_preds, get_model
from libs.cmodel.smart_matching import shorten_cell, smart_matching
from info.module_groups import cmg_map, bmg_map
from libs.read import partly_parse


def auto_module_matching(endpoint, bonus_percent=None, model=None):
    """Full pipline for matching nomenclatures to bonus SKUs.
    Takes client_id and returns url to csv with client's nomenclatures with additional info.

    Input:
        endpoint (str): client's id.
        bonus_percent (float): coefficient as multiplyer of bonus.
        model: CModel.

    Output:
        tuple:
            url (str): url to csv. May be str with error.
            stats (dict): info to log. Duplicate of url in case of error.

    Possible errors in output:
        "error: endpoint must be uuid" - incorrect input format.
        "error: db" - connection to DB.
        "error: no bonus" - no bonus for such client in DB.
        "error: none bonus" - there is bonus but it is empty.
        "error: no noms" - no nomenclatures for such client in DB.
        "error: {code} storage" - error during csv uploading to storage where 'code' is from storage's response.
    """
    try:
        endpoint = str(UUID(endpoint))
    except ValueError:
        error = "error: endpoint must be uuid"
        return error, error
    if (not isinstance(bonus_percent, float)) or (bonus_percent <= 0):
        bonus_percent = 0.3
    
    data = fetch_mm_data(endpoint, bonus_percent)
    if isinstance(data, str):
        return data, data
    elif isinstance(data, tuple):
        train, test = data
    else:
        raise TypeError("unexpected fetch_mm_data response")
    if model is None:
        model = get_model()
    train = add_info(train, model)
    test = add_info(test, model, add_groups=True)
    result = module_matching(train, test, model)
    stats = log_mm_statistics(result, train, endpoint)
    url = result_to_cloud(result, endpoint)
    return url, stats


def add_info(df, model=None, add_groups=False, dict_in=False):
    """Prepares data from DB for matching. Defines trail and manufacturer. Adds groups if needed.

    Input:
        df (pd.DataFrame): must contain "id" (unique) and "x" column.
        model: CModel.
        add_groups (bool): True if need to define module and pharm groups (2 additional columns).
        dict_in (bool): True if df contains x_parsed.

    Output:
        same df (pd.DataFrame) with additional columns: [trail, m_preds],
        where trail is CModel prediction, m_preds is manufacturers id (int).
        adds [m_group, ph_froup] on demand.
    """
    if model is None:
        model = get_model()
    df = partly_parse(df, column='x', dict_in=dict_in, dict_out=True, tailings=True, manuf=True)
    df = get_preds(df, model=model, col_name='x', parsed_back=True, trail=True, parsed_as_dict=True)
    df = df.assign(m_preds=get_manufs(df, parse_dict_in=True, trailed_preds=True, model=model, manual=True))
    if add_groups:
        df = df.assign(
            m_group=df.preds.map(lambda x: get_module_group(x, model)),
            ph_group=df.preds.map(get_pharm_group),
        )
    return df


def module_matching(train, test, model):
    """Matches test to train: for every test row checks if there is same good in train.

    Input:
        train (pd.DataFrame): must contain "id" (unique), "x" and "bonus" column.
        test (pd.DataFrame): must contain "id" (unique), "x", "nomcode", "m_preds", "m_group" and "ph_group" column.

    Output:
        df like test, columns are: ["id", "nomcode", "m_group", "ph_group", "y", "bonus"],
        where "y" is id and "bonus" is bonus for that id from train (in case of succesful matching, else both None).
    """
    bonus_map = train.set_index("id")["bonus"]
    results = smart_matching((train, test), camp_is_brand=False, model=model, manual_mode=True)
    results = results.assign(bonus=results.y.map(bonus_map))
    results.loc[(results.proba != False), "y"] = None
    results.loc[(results.proba != False), "bonus"] = None
    return results[["id", "nomcode", "m_group", "ph_group", "y", "bonus"]]


def log_mm_statistics(result, train, endpoint):
    """Collects statistics of module_matching.

    Input:
        result (pd.DataFrame): output of module_matching.
        train (pd.DataFrame): train used for module_matching to get result.
        endpoint (str): pharmacy (client) id.
    
    Output:
        dict of statistics to log.
    """
    stats = {"endpoint": endpoint}
    # nomenclature list len
    stats["n_nom"] = len(result)
    # bonus list len
    stats["n_bon"] = len(train)
    # bonus list sum
    stats["s_bon"] = round(pd.to_numeric(train.bonus).sum(), 2)
    # quantity of grouped noms (module)
    stats["n_mg_nom"] = result.m_group.map(lambda x: str(x).startswith("9000")).sum()
    # quantity of grouped noms (pharm)
    stats["n_phg_nom"] = result.ph_group.notna().sum()
    test_has_group_mask = result.m_group.duplicated(keep=False) & result.m_group.map(lambda x: x != False)
    # quantity of unique bonuses in nom with groups
    stats["n_bon+"] = len(set(result.y[test_has_group_mask]) - {None})
    # sum of bonuses in nom with groups
    stats["s_bon+"] = round(pd.to_numeric(result.bonus[test_has_group_mask]).sum(), 2)
    no_nom_train_bonus = train.bonus[~train["id"].isin(result.y.unique())]
    # quantity of bonuses not in nom
    stats["n_bon-"] = len(no_nom_train_bonus)
    # sum of bonuses not in nom
    stats["s_bon-"] = round(pd.to_numeric(no_nom_train_bonus).sum(), 2)
    has_nom_no_group = set(result.y[~test_has_group_mask]) - {None}
    # quantity of bonuses in nom but not grouped
    stats["n_bon-gr"] = len(has_nom_no_group)
    # sum of bonuses in nom but not grouped
    stats["s_bon-gr"] = round(pd.to_numeric(train.bonus[train["id"].isin(has_nom_no_group)]).sum(), 2)
    return stats
