from typing import Literal

SB_scenario = Literal[
    "default_1",
    "default_2",
    "default_3",
    "seasonal_1",
    "seasonal_2",
    "seasonal_3",
    "class_specific_seasonal_1",
    "class_specific_seasonal_2",
    "class_specific_seasonal_3",
    "class_specific_seasonal_TOU_1",
    "class_specific_seasonal_TOU_2",
    "class_specific_seasonal_TOU_3",
]

electric_utility = Literal["Coned", "National Grid", "NYSEG"]
