from datetime import timedelta

from rate_design_platform.DecisionMaker import BasicHumanController
from rate_design_platform.utils.rates import TOUParameters

SAMPLE_TOU_PARAMS = TOUParameters(peak_start_hour=timedelta(hours=12), peak_end_hour=timedelta(hours=20))


def test_basic_human_controller():
    human_controller = BasicHumanController()
    """Test human_controller function"""
    # Test from default state with positive savings
    decision = human_controller.TOU_decision("default", 100.0, 80.0, 5.0, SAMPLE_TOU_PARAMS)
    assert decision == "switch"  # Should switch because net savings positive (20 - 3 = 17 > 0)

    # Test from default state with small savings
    decision = human_controller.TOU_decision("default", 100.0, 98.0, 5.0, SAMPLE_TOU_PARAMS)
    assert decision == "stay"  # Should stay because net savings negative (2 - 3 = -1 < 0)

    # Test from TOU state with good realized savings
    decision = human_controller.TOU_decision("tou", 100.0, 80.0, 5.0, SAMPLE_TOU_PARAMS)
    assert decision == "stay"  # Should stay because net savings positive (20 - 5 = 15 > 0)

    # Test from TOU state with poor performance (negative savings)
    decision = human_controller.TOU_decision("tou", 100.0, 110.0, 5.0, SAMPLE_TOU_PARAMS)
    assert decision == "switch"  # Should switch back because net savings negative (-10 - 5 = -15 < 0)

    # Test case: exactly break-even from default
    decision = human_controller.TOU_decision("default", 100.0, 97.0, 5.0, SAMPLE_TOU_PARAMS)
    assert decision == "stay"  # Net savings = 3 - 3 = 0, not > 0, so stay

    # Test case: exactly break-even from TOU
    decision = human_controller.TOU_decision("tou", 100.0, 95.0, 5.0, SAMPLE_TOU_PARAMS)
    assert decision == "stay"  # Net savings = 5 - 5 = 0, not < 0, so stay on TOU

    # Test case: zero comfort penalty
    decision = human_controller.TOU_decision("tou", 100.0, 80.0, 0.0, SAMPLE_TOU_PARAMS)
    assert decision == "stay"  # Net savings = 20 - 0 = 20 > 0
