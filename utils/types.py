from typing import Literal


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


electric_utility = Literal["Coned", "National Grid", "NYSEG"]

gas_utility = Literal["National Grid", "NYSEG", "RIE"]
