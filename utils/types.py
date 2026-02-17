from typing import Literal

# std_name from utility_codes. Keep in sync with get_electric_std_names/get_gas_std_names.
electric_utility = Literal[
    "bath",
    "cenhud",
    "chautauqua",
    "coned",
    "nimo",
    "nyseg",
    "or",
    "psegli",
    "rge",
    "rie",
]
gas_utility = Literal[
    "bath",
    "cenhud",
    "chautauqua",
    "coned",
    "corning",
    "fillmore",
    "kedli",
    "kedny",
    "nfg",
    "nimo",
    "nyseg",
    "or",
    "reserve",
    "rge",
    "rie",
    "stlaw",
    "valley",
    "woodhull",
]


class SBScenario:
    analysis_type: Literal["default", "seasonal", "class_specific_seasonal"]
    analysis_year: int

    def __init__(
        self,
        analysis_type: Literal["default", "seasonal", "class_specific_seasonal"],
        analysis_year: int,
    ):
        if analysis_type not in ["default", "seasonal", "class_specific_seasonal"]:
            raise ValueError(f"Invalid analysis type: {analysis_type}")
        self.analysis_type = analysis_type
        self.analysis_year = analysis_year

    def __str__(self):
        return f"{self.analysis_type}_{self.analysis_year}"
