# Some tools for grabbing the data
import pandas as pd
from typing import Optional, List
import sys
import os

dataset_file = "data/datasets.csv"


def get_all_datasets():
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

    return pd.read_csv(locations[0]).query("Use==1")


def get_ds(mH: Optional[int] = None,
           mS: Optional[int] = None,
           lifetime: Optional[int] = None,
           campaign: Optional[str] = None):
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

    query = '&'.join(q_phrase)

    return get_all_datasets().query(query)
