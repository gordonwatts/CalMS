from calms.data.data import as_single_sample
from calms.data import get_ds, as_samples
from hep_tables import xaod_table


def test_ds_no_args():
    ds = get_ds()
    assert len(ds) > 10


def test_ds_get_one():
    ds = get_ds(mH=125, mS=35, lifetime=5, campaign='mc16e')
    assert len(ds) == 1


def test_ds_get_tag():
    ds = get_ds(tag="highmass")
    assert len(ds) == 3


def test_as_samples():
    ds = get_ds(mH=125, mS=35, lifetime=5, campaign='mc16e')
    s = as_samples(ds)
    assert len(s) == 1
    assert "mH" in s[0]
    assert "mS" in s[0]
    assert "lifetime" in s[0]
    assert 'campaign' in s[0]
    assert 'tags' in s[0]

    assert s[0]["mH"] == 125
    assert s[0]["mS"] == 35
    assert s[0]["lifetime"] == 5
    assert s[0]["campaign"] == 'mc16e'
    assert isinstance(s[0]['data'], xaod_table)


def test_as_single_sample():
    ds = get_ds(tag='highmass')

    sample = as_single_sample(ds)
    assert sample['mS'] == '275'
    assert sample['mH'] == '1000'
    assert sample['lifetime'] == '5'
    assert sample['campaign'] == 'mc16a,mc16d,mc16e'
    assert sample['tags'] == 'signal,highmass'
    assert isinstance(sample['data'], xaod_table)


def test_as_jz_workers():
    ds = get_ds(tag='jz3')

    sample = as_single_sample(ds)
    assert isinstance(sample['data'], xaod_table)
    assert sample['data'].event_source[0]._ds._max_workers == 200


def test_default_get_returns_no_non_signal():
    ds = as_samples(get_ds())
    for d in ds:
        assert 'signal' in d['tags']
