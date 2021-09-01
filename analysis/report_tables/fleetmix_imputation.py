import pandas as pd
from pathlib import Path
from airportei.utilis import PATH_PROCESSED, PATH_INTERIM

path_ops2019_clean = Path.home().joinpath(
    PATH_INTERIM, "ops2019_meta_imputed_cor_counties.xlsx"
)
ops_2019 = pd.read_excel(path_ops2019_clean, index_col=0)
ops_2019_fil = ops_2019[["facility_id", "district_tx_boundar"]]

path_out_flt = Path.home().joinpath(PATH_PROCESSED, "fleet_non_comm_reliev.xlsx")

fleet_non_comm_reliev = pd.read_excel(path_out_flt)

fleet_fil = (
    fleet_non_comm_reliev.filter(
        items=[
            "facility_id",
            "facility_group",
            "filled_from_facility_id",
            "filled_from_facility_ops_per_diff",
            "source",
        ]
    )
    .drop_duplicates(["facility_id"])
    .loc[lambda df: df.source != "TFMSC"]
)
fleet_fil_1 = fleet_fil.merge(
    ops_2019_fil.rename(columns={"district_tx_boundar": "fac_district"}),
    on="facility_id",
    how="left",
)

fleet_fil_2 = fleet_fil_1.merge(
    ops_2019_fil.rename(
        columns={
            "district_tx_boundar": "filled_fac_district",
            "facility_id": "filled_from_facility_id",
        }
    ),
    left_on="filled_from_facility_id",
    right_on="filled_from_facility_id",
    how="left",
)

fleet_fil_med = fleet_fil_2.loc[lambda df: df.facility_group == "Medical"]


fleet_fil_tasp = fleet_fil_2.loc[lambda df: df.facility_group == "TASP"]
fleet_fil_tasp_fil = fleet_fil_tasp[
    [
        "facility_id",
        "fac_district",
        "filled_from_facility_id",
        "filled_fac_district",
        "filled_from_facility_ops_per_diff",
        "source",
    ]
]

path_report_tab_tasp = Path.home().joinpath(
    PATH_INTERIM, "report", "tasp_fleet_imputation.csv"
)
fleet_fil_tasp_fil.to_csv(path_report_tab_tasp)
