# Some tools for grabbing the data
import os
import sys
from typing import Any, Dict, List, Optional

import aiohttp
from func_adl_xAOD import ServiceXDatasetSource
from hep_tables import xaod_table
import pandas as pd
from servicex import ServiceXAdaptor

dataset_file = "calms/data/datasets.csv"


def get_all_datasets() -> pd.DataFrame:
    '''
    Returns a pandas table with a list of all datasets

    Columns:
        mH
        mS
        Lifetime
        MCCampaign
        RucioDSN
        Comments
    '''

    locations = [f for f in [os.path.join(d, dataset_file) for d in sys.path]
                 if os.path.exists(f)]
    if len(locations) == 0:
        raise Exception(f'Unable to find the dataset file {dataset_file}')

    return pd.read_csv(locations[0]).query("Use==1")  # type: ignore


def get_ds(mH: Optional[int] = None,
           mS: Optional[int] = None,
           lifetime: Optional[int] = None,
           campaign: Optional[str] = None) -> pd.DataFrame:
    '''
    Return datasets that satisfy the constraints
    '''
    q_phrase: List[str] = []
    if mH is not None:
        q_phrase.append(f'mH=={mH}')
    if mS is not None:
        q_phrase.append(f'mS=={mS}')
    if lifetime is not None:
        q_phrase.append(f'Lifetime=={lifetime}')
    if campaign is not None:
        q_phrase.append(f'MCCampaign=="{campaign}"')

    all_ds = get_all_datasets()
    return all_ds if len(q_phrase) == 0 \
        else all_ds.query('&'.join(q_phrase))  # type: ignore


def as_samples(datasets: pd.DataFrame, client: Optional[aiohttp.ClientSession] = None) \
            -> List[Dict[str, Any]]:
    '''
    Given a pandas dataframe that was pulled from `get_ds`, return a similar
    dict, with one entry containing xaod_table.
    '''
    def convert(row_data, sx_adaptor=None):
        from servicex import ServiceXDataset
        sd = ServiceXDataset(row_data.RucioDSName, servicex_adaptor=sx_adaptor,
                             image="sslhep/servicex_func_adl_xaod_transformer:v0.4Update")
        return {
            'mS': float(row_data.mS),
            'mH': float(row_data.mH),
            'lifetime': float(row_data.Lifetime),
            'campaign': row_data.MCCampaign,
            'data': xaod_table(ServiceXDatasetSource(sd))
        }

    return [convert(row_data) for row_data in datasets.itertuples()]
