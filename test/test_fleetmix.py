import pandas as pd
import numpy as np
from pathlib import Path
from airportei.utilis import PATH_PROCESSED, PATH_INTERIM

path_ops2019_clean = Path.home().joinpath(
    PATH_INTERIM, "ops2019_meta_imputed_cor_counties.xlsx"
)
ops_2019 = pd.read_excel(path_ops2019_clean, index_col=0)
ops_2019_fil = ops_2019.loc[
    lambda df: (df.facility_group.isin(["Commercial", "Reliever"]))
]

path_out_emis_comm_rel = Path.home().joinpath(PATH_PROCESSED, "emis_comm_reliev.xlsx")
path_out_flt = Path.home().joinpath(PATH_PROCESSED, "fleet_comm_reliev.xlsx")
path_out_emis = Path.home().joinpath(PATH_PROCESSED, "emis_non_comm_reliev.xlsx")
path_out_flt = Path.home().joinpath(PATH_PROCESSED, "fleet_non_comm_reliev.xlsx")
