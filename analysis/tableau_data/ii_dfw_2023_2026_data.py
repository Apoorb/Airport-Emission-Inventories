"""
Get DFW data.
"""
import time
import re
from pathlib import Path
import pandas as pd
import numpy as np
import inflection

if __name__ == "__main__":
    path_com = Path(
        r"C:\Users\a-bibeka\OneDrive - Texas A&M Transportation Institute"
        r"\Documents\Projects\Airports\Data Analysis\Data"
        r"\uncntr_cntr_emis_raw_data"
    )
    path_nfdc = Path.home().joinpath(path_com.parent, "FAA_NFDC_Facilities.csv")
    path_fac_scc = Path.home().joinpath(path_com.parent, "emis_fac_scc.csv")
    path_dfw_out = Path.home().joinpath(path_com.parent, "dfw_emis_data.xlsx")

    emis_fac_scc = pd.read_csv(path_fac_scc)
    emis_fac_scc_dfw = emis_fac_scc.loc[lambda df: df.Facility == "dfw"]
    emis_fac_scc_dfw.to_excel(path_dfw_out, index=False)
