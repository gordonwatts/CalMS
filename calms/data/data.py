# Some tools for grabbing the data
import os
import sys
from typing import Any, Dict, List, Optional

import aiohttp
from func_adl_xAOD import ServiceXDatasetSource
from hep_tables import xaod_table
import pandas as pd

dataset_file = "calms/data/datasets.csv"
servicex_image = "sslhep/servicex_func_adl_xaod_transformer:v0.4update"


def get_all_datasets() -> pd.DataFrame:
    '''
    Returns a pandas table with a list of all datasets

    Columns:
        mH
        mS
        Lifetime
        MCCampaign
        RucioDSN
        Tags
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
           campaign: Optional[str] = None,
           tag: Optional[str] = "signal") -> pd.DataFrame:
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
    if tag is not None:
        q_phrase.append(f'Tags.str.contains("{tag}")')

    all_ds = get_all_datasets()
    return all_ds if len(q_phrase) == 0 \
        else all_ds.query('&'.join(q_phrase))  # type: ignore


sx_args = {
    'jetjet': {'max_workers': 200}
}


def _make_sxds(ds_name: str, tags: str):
    '''
    Internal method to create the sx dataset.
    Uses tags to find extra arguments to pass to the config of the dataset.
    '''
    # Are there extra arguments?
    tags = tags.split(',')
    args = {}
    for t in tags:
        if t in sx_args:
            args.update(sx_args[t])

    from servicex import ServiceXDataset
    return ServiceXDatasetSource(ServiceXDataset(ds_name, image=servicex_image, **args))


def as_samples(datasets: pd.DataFrame) \
            -> List[Dict[str, Any]]:
    '''
    Given a pandas dataframe that was pulled from `get_ds`, return a similar
    dict, with one entry containing xaod_table.
    '''
    def convert(row_data):
        return {
            'mS': float(row_data.mS),
            'mH': float(row_data.mH),
            'lifetime': float(row_data.Lifetime),
            'campaign': row_data.MCCampaign,
            'tags': row_data.Tags,
            'data': xaod_table(_make_sxds(row_data.RucioDSName, row_data.Tags))
        }

    return [convert(row_data) for row_data in datasets.itertuples()]


def _nice_format(o: Any) -> str:
    if isinstance(o, float):
        f = str(o)
        if f.endswith('.0'):
            return f[:-2]
        return f
    return str(o)


def _combine_values(datasets: pd.DataFrame, column_name: str):
    a = set([getattr(row_data, column_name) for row_data in datasets.itertuples()])
    return ','.join(sorted([_nice_format(item) for item in a]))


def as_single_sample(datasets: pd.DataFrame) -> List[Dict[str, Any]]:
    '''
    Given a list of datasets, return them as a single `xaod_table` object.
    All samples have the same weight.
    '''

    return {
        'mS': _combine_values(datasets, 'mS'),
        'mH': _combine_values(datasets, 'mH'),
        'lifetime': _combine_values(datasets, 'Lifetime'),
        'campaign': _combine_values(datasets, 'MCCampaign'),
        'tags': _combine_values(datasets, 'Tags'),
        'data': xaod_table(*[_make_sxds(d.RucioDSName, d.Tags) for d in datasets.itertuples()])
    }
