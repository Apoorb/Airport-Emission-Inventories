import numpy as np
import pandas as pd
from copy import deepcopy
import lxml
from lxml import etree
from pathlib import Path
import pathlib
from airportei.utilis import PATH_INTERIM

DEFAULT_APU_TIME = 13.0


class AsifXml:
    def __init__(
            self,
            path_inputs_: pathlib.WindowsPath,
            path_xml_templ_: pathlib.WindowsPath,
            analysis_arpt_: str):
        self.path_inputs = path_inputs_
        self.path_xml_templ = path_xml_templ_
        self.analysis_arpt = analysis_arpt_
        self.asif_tre = lxml.etree._ElementTree()
        self.asif_rt = lxml.etree._Element()
        self.elem_before_trackopset = lxml.etree._Element()
        self.asif_trackopset_a = lxml.etree._Element()
        self.asif_trackopset_d = lxml.etree._Element()
        self.asif_trackopset_dict = {}
        self.asif_annualzn = lxml.etree._Element()
        self.trk = pd.DataFrame
        self.layout = pd.DataFrame
        self.flt = pd.DataFrame
        self.acftops = pd.DataFrame
        self.heliops = pd.DataFrame
        self.hasheli = False
        self.nm = f"{analysis_arpt_}_2019"
        self.starttime = "2019-01-01T07:00:00"
        self.dur = "24"
        self.taximod = "UserSpecified"
        self.acftPerfModel = "SAE1845"
        self.bankAngle = "true"
        self.description = f"{analysis_arpt_} 2019 Emissions"
        self.layoutname = ""
        self.case_nm = self.nm + "_case"
        self.case_src = "Aircraft"
        self.aputime = str(13.0)
        self.set_tree_trk_layout_ops()

    def set_tree_trk_layout_ops(self) -> None:
        parser = etree.XMLParser(remove_blank_text=True)
        self.asif_tre = etree.parse(str(self.path_xml_templ), parser)
        self.asif_rt = self.asif_tre.getroot()
        self.elem_before_trackopset = self.asif_tre.find(".//case//duration")
        self.asif_trackopset_a = self.asif_rt.xpath(
            ".//optype[text()='A']/ancestor::trackOpSet")[0]
        self.asif_trackopset_d = self.asif_rt.xpath(
            ".//optype[text()='D']/ancestor::trackOpSet")[0]
        self.asif_trackopset_dict = {
            "A": self.asif_trackopset_a,
            "D": self.asif_trackopset_d,
        }
        self.asif_annualzn = self.asif_tre.find(".//annualization")
        x_asif_in = pd.ExcelFile(self.path_inputs)
        self.layout = x_asif_in.parse("layout", index_col=0)
        self.trk = x_asif_in.parse("track", index_col=0)
        self.flt = x_asif_in.parse("fleet", index_col=0)
        self.flt = (
            self.flt
            .sort_values(["ltos", "arfm_mod", "engine_code", "op_type"],
                             ascending=[False, True, True, True])
            .reset_index(drop=True)
            .assign(ids=lambda df: (np.floor(df.index / 2) + 1).astype(int))
        )
        self.heliops = self.flt.loc[lambda df: ~ df.anp_helicopter_id.isna()]
        self.acftops = self.flt.loc[lambda df: df.anp_helicopter_id.isna()]
        if len(self.heliops) != 0:
            self.hasheli = True

    def set_scn_meta(self, ) -> None:
        scn_attr = {
            "name": self.nm,
            "startTime": self.starttime,
            "duration": self.dur,
            "taxiModel": self.taximod,
            "acftPerfModel": self.acftPerfModel,
            "bankAngle": self.bankAngle,
            "description": self.description,
            "airportLayoutName": self.layout.name.values[0],
        }
        for key, val in scn_attr.items():
            self.asif_tre.find(f".//{key}").text = val

    def set_case_meta(self) -> None:
        case_meta_attr = {
            "case//caseId": str(0),
            "case//name": self.case_nm,
            "case//source": self.case_src,
            "case//startTime": self.starttime,
            "case//duration": self.dur
        }
        for key, val in case_meta_attr.items():
            self.asif_tre.find(f".//{key}").text = val

    def set_trackopsets(self) -> None:
        self.asif_trackopset_dict["A"].getparent().remove(self.asif_trackopset_dict["A"])
        self.asif_trackopset_dict["D"].getparent().remove(self.asif_trackopset_dict["D"])
        aircraft_types = {
            "aircraft": 0,
            "helicopter": 1
        }
        for aircraft_type in aircraft_types.values():
            if (aircraft_type == 1) and (not self.hasheli):
                continue
            for op_type in ["A", "D"]:
                if aircraft_type == 0:
                    ops = self.acftops
                elif aircraft_type == 1:
                    ops = self.heliops
                else:
                    raise ValueError("aircraft_type should be either 0 or 1.")
                ops_fil = (
                    ops.loc[lambda df: (df.op_type == op_type)]
                )
                trk_fil = self.trk.loc[lambda df: (df.aircraft_type ==
                                              aircraft_type) & (df.op_type == op_type)]
                asif_trackopset_dcpy = deepcopy(self.asif_trackopset_dict[op_type])
                self.set_track(trk_fil_=trk_fil,
                               asif_trackopset_dcpy_=asif_trackopset_dcpy)
                self.set_ops(op_type_=op_type,
                    ops_fil_=ops_fil,
                             trk_fil_=trk_fil,
                             asif_trackopset_dcpy_=asif_trackopset_dcpy)
                self.elem_before_trackopset.addnext(asif_trackopset_dcpy)
                self.set_annualization()


    def set_track(
            self,
            trk_fil_: pd.DataFrame,
            asif_trackopset_dcpy_: lxml.etree._Element) -> None:
        asif_trackopset_dcpy_.find(".//name").text = trk_fil_.track_name.values[0]
        asif_trackopset_dcpy_.find(".//optype").text = trk_fil_.op_type.values[0]
        asif_trackopset_dcpy_.find(".//airport").text = self.analysis_arpt
        asif_trackopset_dcpy_.find(".//runway").text = trk_fil_.rwy_end_name.values[0]
        asif_trackopset_dcpy_.find(".//type").text = trk_fil_.segment_type.values[0]
        asif_trackopset_dcpy_.find(".//distance").text = str(trk_fil_.dist_or_rad.values[0])
        assert trk_fil_.turn_angle.values[0] == 0, (
            "Handle non-zero turn "
            "angle.")

    def set_ops(
            self,
            op_type_: str,
            ops_fil_: pd.DataFrame,
            trk_fil_: pd.DataFrame,
            asif_trackopset_dcpy_: lxml.etree._Element) -> None:
        asif_ops = asif_trackopset_dcpy_.find(".//operations")
        asif_op = asif_trackopset_dcpy_.find(".//operation")
        asif_op_dcpy = deepcopy(asif_op)
        # Remove the operation from the template. Will replace them with the
        # correct operation for each aircraft in the fleetmix.
        asif_ops.remove(asif_op)
        for idx, row in ops_fil_.iterrows():
            asif_op_dcpy_dcpy = deepcopy(asif_op_dcpy)
            asif_op_dcpy_dcpy.find(".//id").text = op_type_ + str(row.ids)
            asif_op_dcpy_dcpy.find(".//airframeModel").text = row.arfm_mod
            asif_op_dcpy_dcpy.find(".//engineCode").text = row.engine_code
            if row.apu_name != row.apu_name:
                asif_op_dcpy_dcpy.find(".//apuName").getparent().remove(
                    asif_op_dcpy_dcpy.find(".//apuName")
                )
                if op_type_ == "A":
                    asif_op_dcpy_dcpy.find(".//arrivalApuTime").getparent().remove(
                        asif_op_dcpy_dcpy.find(".//arrivalApuTime")
                    )
                else:
                    asif_op_dcpy_dcpy.find(".//departureApuTime").getparent().remove(
                        asif_op_dcpy_dcpy.find(".//departureApuTime")
                    )
            else:
                asif_op_dcpy_dcpy.find(".//apuName").text = row.apu_name
                if op_type_ == "A":
                    asif_op_dcpy_dcpy.find(".//arrivalApuTime").text = str(
                        self.aputime)
                else:
                    asif_op_dcpy_dcpy.find(".//departureApuTime").text = str(
                        self.aputime)
            asif_op_dcpy_dcpy.find(".//numOperations").text = str(row.ltos)
            asif_op_dcpy_dcpy.find(".//opType").text = row.op_type
            if op_type_ == "A":
                asif_op_dcpy_dcpy.find(".//arrivalAirport").text = self.analysis_arpt
                asif_op_dcpy_dcpy.find(".//arrivalRunway").text = trk_fil_.rwy_end_name.values[0]
                asif_op_dcpy_dcpy.find(".//onTime").text = self.starttime
            else:
                asif_op_dcpy_dcpy.find(".//departureAirport").text = self.analysis_arpt
                asif_op_dcpy_dcpy.find(".//departureRunway").text = trk_fil_.rwy_end_name.values[0]
                asif_op_dcpy_dcpy.find(".//offTime").text = self.starttime
            asif_op_dcpy_dcpy.find(".//saeProfile").text = row.profile
            asif_op_dcpy_dcpy.find(".//stageLength").text = str(row.stage_len)
            asif_ops.append(asif_op_dcpy_dcpy)

    def set_annualization(self) -> None:
        self.asif_annualzn.find(".//name").text = "baseline"
        self.asif_annualzn.find(".//annualizationCase//name").text = self.case_nm

    def write_asif(self, path_out: pathlib.WindowsPath) -> None:
        self.asif_tre.write(str(path_out), pretty_print=True)
        ...

    def getasifxml(self, path_inputs: str, path_xmltempl: str
                   ) -> etree._ElementTree:
        ...


if __name__ == "__main__":
    analysis_arpt = "KIAH"
    path_xml_temp = Path.home().joinpath(PATH_INTERIM,
                                         "template_asif_scenario.xml")
    iah_asif_input_fi = Path.home().joinpath(PATH_INTERIM,
                                             "iah_asif_input.xlsx")
    path_asif_out = Path.home().joinpath(
        PATH_INTERIM, "asif_xmls", "test_kiah_asif.xml")
    asifxml_obj = AsifXml(path_inputs_=iah_asif_input_fi,
                          path_xml_templ_=path_xml_temp,
                          analysis_arpt_=analysis_arpt)
    asifxml_obj.set_tree_trk_layout_ops()
    asifxml_obj.set_scn_meta()
    asifxml_obj.set_case_meta()
    asifxml_obj.set_trackopsets()
    asifxml_obj.set_annualization()
    asifxml_obj.write_asif(path_out=path_asif_out)

