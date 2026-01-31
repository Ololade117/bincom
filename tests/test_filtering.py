import pytest
from data import build_polling_unit_results_df, get_lgas_by_state, get_wards_by_lga, filter_results

DF = build_polling_unit_results_df('bincom_test.sql')


def test_lga_and_ward_filtering():
    lgas = get_lgas_by_state(DF)
    assert any('Aniocha' in name for (_id, name) in lgas)

    lid = next(_id for _id, name in lgas if 'Aniocha North' in name)
    wards = get_wards_by_lga(DF, lga_id=lid)
    assert len(wards) > 0

    wid = wards[0][0]
    filtered = filter_results(DF, ward_id=wid)
    assert not filtered.empty


def test_polling_unit_scoping():
    lgas = get_lgas_by_state(DF)
    lid = next(_id for _id, name in lgas if 'Aniocha' in name)

    wards = get_wards_by_lga(DF, lga_id=lid)
    assert len(wards) > 0
    wid = wards[0][0]

    pus = DF[(DF['lga_id'] == lid) & (DF['ward_id'] == wid)][['polling_unit_uniqueid', 'polling_unit_name']].drop_duplicates()
    assert not pus.empty

    pu_id = int(pus['polling_unit_uniqueid'].iloc[0])
    filtered = filter_results(DF, polling_unit_id=pu_id)
    assert not filtered.empty
