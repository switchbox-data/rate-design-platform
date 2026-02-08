from typing import Literal


class SB_scenario:
    analysis_type: Literal["default", "seasonal", "class_specific_seasonal"]
    analysis_year: int

    def __init__(
        self,
        analysis_type: Literal["default", "seasonal", "class_specific_seasonal"],
        analysis_year: int,
    ):
        self.analysis_type = analysis_type
        self.analysis_year = analysis_year

    def __str__(self):
        return f"{self.analysis_type}_{self.analysis_year}"


electric_utility = Literal["Coned", "National Grid", "NYSEG"]

gas_utility = Literal["National Grid", "NYSEG"]
