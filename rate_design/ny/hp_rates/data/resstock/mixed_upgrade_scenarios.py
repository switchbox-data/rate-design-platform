from buildstock_fetch.mixed_upgrade import MixedUpgradeScenario
from buildstock_fetch.scenarios import uniform_adoption

data_path = "/data.sb/nrel/resstock/"

scenario = uniform_adoption(
    upgrade_ids=[4, 8],
    weights={4: 0.6, 8: 0.4},
    adoption_trajectory=[0.1, 0.3, 0.5],
)

mus = MixedUpgradeScenario(
    data_path=data_path,
    # scenario_name="test_mus",
    release="res_2024_amy2018_2",
    states="NY",
    sample_n=5,
    random=42,
    scenario=scenario,
)