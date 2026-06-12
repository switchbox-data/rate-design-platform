from typing import Literal

# std_name from utility_codes. Keep in sync with get_electric_std_names/get_gas_std_names.
ElectricUtility = Literal[
    "bath",
    "bge",
    "berlin_muni",
    "cenhud",
    "chautauqua",
    "choptank",
    "coned",
    "delmarva",
    "easton_muni",
    "hagerstown_muni",
    "nimo",
    "nyseg",
    "or",
    "pepco",
    "potomac_edison",
    "psegli",
    "rge",
    "rie",
    "smeco",
    "somerset_rec",
]
GasUtility = Literal[
    "bath",
    "bge",
    "cenhud",
    "chautauqua",
    "chesapeake_utilities",
    "columbia_gas_md",
    "coned",
    "corning",
    "easton_muni",
    "elkton_gas",
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
    "sandpiper",
    "stlaw",
    "ugi_central_penn",
    "valley",
    "washington_gas",
    "woodhull",
]


class SBScenario:
    analysis_type: Literal[
        "default", "seasonal", "seasonal_discount", "class_specific_seasonal"
    ]
    analysis_year: int

    def __init__(
        self,
        analysis_type: Literal[
            "default", "seasonal", "seasonal_discount", "class_specific_seasonal"
        ],
        analysis_year: int,
    ):
        if analysis_type not in [
            "default",
            "seasonal",
            "seasonal_discount",
            "class_specific_seasonal",
        ]:
            raise ValueError(f"Invalid analysis type: {analysis_type}")
        self.analysis_type = analysis_type
        self.analysis_year = analysis_year

    def __str__(self):
        return f"{self.analysis_type}_{self.analysis_year}"
