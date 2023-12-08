from integrations import defs

def test_definitions_loaded():
    assert len(defs.get_asset_graph().assets) > 0, "There should be at least one asset defined"
    assert len(defs.get_all_job_defs()) > 0, "There should be at least one job defined"