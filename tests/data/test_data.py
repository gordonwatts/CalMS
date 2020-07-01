from calms.data import get_ds, as_samples
from hep_tables import xaod_table


def test_ds_no_args():
    ds = get_ds()
    assert len(ds) == 57


def test_ds_get_one():
    ds = get_ds(mH=125, mS=35, lifetime=5, campaign='mc16e')
    assert len(ds) == 1


def test_as_samples():
    ds = get_ds(mH=125, mS=35, lifetime=5, campaign='mc16e')
    s = as_samples(ds)
    assert len(s) == 1
    assert "mH" in s[0]
    assert "mS" in s[0]
    assert "lifetime" in s[0]
    assert 'campaign' in s[0]

    assert s[0]["mH"] == 125
    assert s[0]["mS"] == 35
    assert s[0]["lifetime"] == 5
    assert s[0]["campaign"] == 'mc16e'
    assert isinstance(s[0]['data'], xaod_table)
