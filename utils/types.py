from typing import Literal

# std_name from utility_codes. Keep in sync with get_electric_std_names/get_gas_std_names.
ElectricUtility = Literal[
    "bath",
    "bge",
    "berlin_muni",
    "bozrah_muni",
    "cenhud",
    "chautauqua",
    "choptank",
    "clp",
    "coned",
    "dpl",
    "easton_muni",
    "frp",
    "groton_muni",
    "hagerstown_muni",
    "jewett_muni",
    "mohegan_tribal",
    "nimo",
    "norwich_muni",
    "norwalk_third_taxing",
    "nyseg",
    "or",
    "pepco",
    "poted",
    "psegli",
    "rge",
    "rie",
    "smeco",
    "somerset_rec",
    "south_norwalk_muni",
    "ui",
    "wallingford_muni",
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
    "ct_natural_gas",
    "easton_muni",
    "elkton_gas",
    "fillmore",
    "kedli",
    "kedny",
    "nfg",
    "nimo",
    "norwich_muni",
    "nyseg",
    "or",
    "reserve",
    "rge",
    "rie",
    "sandpiper",
    "southern_ct_gas",
    "stlaw",
    "ugi_central_penn",
    "valley",
    "washington_gas",
    "woodhull",
    "yankee_gas",
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
