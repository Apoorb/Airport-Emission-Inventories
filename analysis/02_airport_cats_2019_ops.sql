DROP SCHEMA IF EXISTS fy21tceq_airportei;

CREATE SCHEMA FY21TCEQ_AirportEI;

-- Get ERG 2017 Data
FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.2017ERG;
create table FY21TCEQ_AirportEI.2017ERG(
FacilityID char(10), 
Airport char(50),
COUNTY char(25),
FIP Int,
SCC Int,
SCCDescription char(50),
AETC Int,
Airframe char(25),
Engine char(25),
Mode char(25),
LTO Float,
EIS_Pollutant_ID char(25),
UNCONTROLLED_ANNUAL_EMIS_ST Float,
CONTROLLED_ANNUAL_EMIS_ST Float,
UNCONTROLLED_DAILY_EMIS_ST Float,
CONTROLLED_DAILY_EMIS_ST Float);

FLUSH TABLES;
LOAD DATA LOCAL INFILE 'C:/Users/a-bibeka/PycharmProjects/airport_ei/data/raw/madhu_files/ERG_2017.csv' INTO
TABLE FY21TCEQ_AirportEI.2017ERG
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\r\n'
ignore 1 lines;

-- Get TxDOT Planning Data
FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.TxDOT_PlanningAirports;
create table FY21TCEQ_AirportEI.TxDOT_PlanningAirports(
FacilityID char(10),
Region_ID char(5),
TxDOT char(5),
Airport_Name char(100),
City char(50),
AirportType Char(20));

FLUSH TABLES;
LOAD DATA LOCAL INFILE 'C:/Users/a-bibeka/PycharmProjects/airport_ei/data/raw/madhu_files/TxDOT_Airports.csv' INTO
TABLE FY21TCEQ_AirportEI.TxDOT_PlanningAirports
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\r\n'
ignore 1 lines;

-- Get TAF 1990 to 2045 Projection Data
FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.TAF1990_2045;
create table FY21TCEQ_AirportEI.TAF1990_2045(
FacilityID char(10),
Region char(5),
APORT_NAME char(100),
CITY char(100),
STATE char(2),
FAC_TYPE Int,
ATCT_FLAG Int,
HUB_SIZE char(2),
SYSYEAR int,
SCENARIO Int,
AC Int,
COMMUTER Int,
T_ENPL Int,
ITN_AC Int,
ITN_AT Int,
ATN_GA Int,
_ITN_MIL Int,
T_ITN Int,
LOC_GA Int,
LOC_MIL Int,
T_LOC Int,
T_AOPS Int,
T_TROPS Int,
TOT_BA Int);

FLUSH TABLES;
LOAD DATA LOCAL INFILE 'C:/Users/a-bibeka/PycharmProjects/airport_ei/data/raw/madhu_files/TX_TAF1990_2045.csv' INTO
TABLE FY21TCEQ_AirportEI.TAF1990_2045
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\r\n'
ignore 1 lines;

-- Get 5010/ NFDC Data
FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.TX_AllAirports;
create table FY21TCEQ_AirportEI.TX_AllAirports(
SiteNumber char(10),
FacilityType char(20),
FacilityID char(10),
EffectiveDate char(12),
Region char(3),
DistrictOffice char(5),
State char(2),
StateName char(10),
County char(50),
CountyState char(2),
City char(50),
FacilityName char(100),
Ownership char(2),
Used char(2),
Owner char(50),
OwnerAddress char(50),
OwnerCSZ char(50),
OwnerPhone char(20),
Manager char(50),
ManagerAddress char(50),
ManagerCSZ char(50),
ManagerPhone char(50),
ARPLatitude char(25),
ARPLatitudeS char(25),
ARPLongitude char(25),
ARPLongitudeS char(25),
ARPMethod char(2),
ARPElevation Int,
ARPElevationMethod char(2),
MagneticVariation char(5),
MagneticVariationYear Int,
TrafficPatternAltitude Int,
ChartName char(50),
DistanceFromCBD int,
DirectionFromCBD char(5),
LandAreaCoveredByAirport Int,
BoundaryARTCCID char(5),
BoundaryARTCCComputerID char(5),
BoundaryARTCCName char(50),
ResponsibleARTCCID char(5),
ResponsibleARTCCComputerID char(5),
ResponsibleARTCCName char(50),
TieInFSS char(5),
TieInFSSID char(50),
TieInFSSName char(20),
AirportToFSSPhoneNumber char(20),
TieInFSSTollFreeNumber char(20),
AlternateFSSID char(20),
AlternateFSSName char(20),
AlternateFSSTollFreeNumber char(20),
NOTAMFacilityID char(5),
NOTAMService char(2),
ActivationDate char(12),
AirportStatusCode char(2),
CertificationTypeDate char(25),
FederalAgreements char(10),
AirspaceDetermination char(25),
CustomsAirportOfEntry char(2),
CustomsLandingRights char(2),
MilitaryJointUse char(2),
MilitaryLandingRights char(2),
InspectionMethod char(2),
InspectionGroup char(2),
LastInspectionDate char(12),
LastOwnerInformationDate char(12),
FuelTypes char(20),
AirframeRepair char(10),
PowerPlantRepair char(10),
BottledOxygenType char(10),
BulkOxygenType char(10),
LightingSchedule char(10),
BeaconSchedule char(10),
ATCT char(2),
UNICOMFrequencies float,
CTAFFrequency float,
SegmentedCircle char(2),
BeaconColor char(5),
NonCommercialLandingFee char(2),
MedicalUse char(2),
SingleEngineGA float,
MultiEngineGA float,
JetEngineGA float,
HelicoptersGA float,
GlidersOperational float,
MilitaryOperational float,
Ultralights float,
OperationsCommercial float,
OperationsCommuter float,
OperationsAirTaxi float,
OperationsGALocal float,
OperationsGAItin float,
OperationsMilitary float,
OperationsDate char(12),
AirportPositionSource char(25),
AirportPositionSourceDate char(12),
AirportElevationSource char(25),
AirportElevationSourceDate char(12),
ContractFuelAvailable char(10),
TransientStorage char(25),
OtherServices char(50),
WindIndicator char(5),
IcaoIdentifier char(10),
MinimumOperationalNetwork char(2));

FLUSH TABLES;
LOAD DATA LOCAL INFILE 'C:/Users/a-bibeka/PycharmProjects/airport_ei/data/raw/madhu_files/FAA_NFDC_Facilities.csv' INTO
TABLE FY21TCEQ_AirportEI.TX_AllAirports
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\r\n'
ignore 1 lines ;

-- Get 2019 TFMSC Data
FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.FAA_2019TFMSC;
create table FY21TCEQ_AirportEI.FAA_2019TFMSC(
Date Char(10),
yearid int,
monthname char(25),
Season char(20),
FacilityID char(5),
Airport char(100),
City Char(50),
FlightType Char(25),
UserClass Char(25),
WeightClass Char(50),
PhysicalClass Char(25),
AircraftID Char(10),
AircraftTypeID VarChar(100),
AircraftType Char(100),
BusinessJet Char(10),
RegionalJet Char(10),
BusinessAviation Char(10),
AirplaneApproachCat Char(10),
AirplaneDesignGrp Char(10),
TaxiwayDesignGrp Char(10),
Departures Int,
Arrivals Int,
TotalOps Int,
DepartureSeats Int,
AverageDepatureSeats Int,
ArrivalSeats Int,
AverageArrivalSeats Int);

FLUSH TABLES;
LOAD DATA LOCAL INFILE 'C:/Users/a-bibeka/PycharmProjects/airport_ei/data/raw/madhu_files/FAA_2019TFMSC.csv' INTO
TABLE FY21TCEQ_AirportEI.FAA_2019TFMSC
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\r\n'
ignore 1 lines;

-- Get 2020 TFMSC Data
FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.FAA_2020TFMSC;
create table FY21TCEQ_AirportEI.FAA_2020TFMSC(
Date Char(10),
yearid int,
monthname char(25),
Season char(20),
FacilityID char(5),
Airport char(100),
City Char(50),
FlightType Char(25),
UserClass Char(25),
WeightClass Char(50),
PhysicalClass Char(25),
AircraftID Char(10),
AircraftTypeID VarChar(100),
AircraftType Char(100),
BusinessJet Char(10),
RegionalJet Char(10),
BusinessAviation Char(10),
AirplaneApproachCat Char(10),
AirplaneDesignGrp Char(10),
TaxiwayDesignGrp Char(10),
Departures Int,
Arrivals Int,
TotalOps Int,
DepartureSeats Int,
AverageDepatureSeats Int,
ArrivalSeats Int,
AverageArrivalSeats Int);

FLUSH TABLES;
LOAD DATA LOCAL INFILE 'C:/Users/a-bibeka/PycharmProjects/airport_ei/data/raw/madhu_files/FAA_2020TFMSC.csv' INTO
TABLE FY21TCEQ_AirportEI.FAA_2020TFMSC
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\r\n'
ignore 1 lines;

-- Get 2019 Taxitime
FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.FAA_2019taxitime;
create table FY21TCEQ_AirportEI.FAA_2019taxitime(
FacilityID char(10),
Date char(20),
Year Int,
Monthname char(15),
Season Char(20),
Hour Int,
Quarter Int,
Departures_Metrics Int,
Avg_Taxiouttime float,
Dep_LT_20 float,
Dep20_39 float,
DEP40_59 float,
Dep60_119 float,
Dep120_179 float,
Dep_180 float,
Dep_40 float,
Dep_40_Pct float,
Dep_60 float,
Dep_60_Pct float,
Dep_90 float,
Arrivals_metrics float,
Avg_Taxiintime float,
Arrv_LT_15 float,
Arrv15_29 float,
Arrv30_44 float,
Arrv45_59 float,
Arrv60_74 float,
Arrv_75 float,
Arrv_30 float,
Arrv_30_Pct float);

FLUSH TABLES;
LOAD DATA LOCAL INFILE 'C:/Users/a-bibeka/PycharmProjects/airport_ei/data/raw/madhu_files/2019_ASPM_Taxitime.csv' INTO
TABLE FY21TCEQ_AirportEI.FAA_2019taxitime
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\n'
ignore 1 lines;

-- Get 2020 Taxitime
FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.FAA_2020taxitime;
create table FY21TCEQ_AirportEI.FAA_2020taxitime(
FacilityID char(10),
Date char(20),
Year Int,
Monthname char(15),
Season Char(20),
Hour Int,
Quarter Int,
Departures_Metrics Int,
Avg_Taxiouttime float,
Dep_LT_20 float,
Dep20_39 float,
DEP40_59 float,
Dep60_119 float,
Dep120_179 float,
Dep_180 float,
Dep_40 float,
Dep_40_Pct float,
Dep_60 float,
Dep_60_Pct float,
Dep_90 float,
Arrivals_metrics float,
Avg_Taxiintime float,
Arrv_LT_15 float,
Arrv15_29 float,
Arrv30_44 float,
Arrv45_59 float,
Arrv60_74 float,
Arrv_75 float,
Arrv_30 float,
Arrv_30_Pct float);

FLUSH TABLES;
LOAD DATA LOCAL INFILE 'C:/Users/a-bibeka/PycharmProjects/airport_ei/data/raw/madhu_files/2020_ASPM_Taxitime.csv' INTO
TABLE FY21TCEQ_AirportEI.FAA_2020taxitime
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\n'
ignore 1 lines;

-- Get Airnav Data
FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.airnav;
create table FY21TCEQ_AirportEI.airnav(
FacilityID char(10),
Single_engine Float,
Multi_engine Float,
Jets  Float,
Helicopters Float,
Gliders  Float,
Ultralights Float,
Military_aircraft Float,
Total_Aircraft Float,
Reported_operations Char(100),
Extracted_Num Float,
Frequency_Multiplier Float,
Annual_Operations Float,
Operations_Year Int,
MilitaryOps Float,
TransientGAOps Float,
LocalGAOps Float,
CommercialOps Float,
AirTaxiOps Float);

FLUSH TABLES;
LOAD DATA LOCAL INFILE 'C:/Users/a-bibeka/PycharmProjects/airport_ei/data/raw/madhu_files/airnav.csv' INTO
TABLE FY21TCEQ_AirportEI.airnav
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\n'
ignore 1 lines;

-- Get TTI Ops Data
FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.TTIOps;
create table FY21TCEQ_AirportEI.TTIOps(
FacilityID Char(10),
2019TTIOps Float,
2020TTIops Float,
2019TTISumOps Float,
2020TTISumOps Float);

FLUSH TABLES;
LOAD DATA LOCAL INFILE 'C:/Users/a-bibeka/PycharmProjects/airport_ei/data/raw/madhu_files/TTIOps.csv' INTO
TABLE FY21TCEQ_AirportEI.TTIOps
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\n'
ignore 1 lines;

-- Get 2019 TTI Ops Data
FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.2019TTIOps;
create table FY21TCEQ_AirportEI.2019TTIOps(
FacilityID Char(10),
FacilityName Char(100),
County Char(100),
FacilityGroup Char(20),
AnnualOps Float,
SummerOps Float);

FLUSH TABLES;
LOAD DATA LOCAL INFILE 'C:/Users/a-bibeka/PycharmProjects/airport_ei/data/raw/madhu_files/2019TTIOps.csv' INTO
TABLE FY21TCEQ_AirportEI.2019TTIOps
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\n'
ignore 1 lines;

-- Get ERG Ops
FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.2017ergOps;
CREATE TABLE FY21TCEQ_AirportEI.2017ergOps
SELECT FacilityID, Airport, COUNTY, 
SUM(IF(SCCDescription="General Aviation, Piston", LTO, 0))*2 AS GeneralAviation_Piston,
SUM(IF(SCCDescription="General Aviation, Turbine", LTO, 0))*2 AS GeneralAviation_Turbine,
SUM(IF(SCCDescription="Air Taxi, Piston", LTO, 0))*2 AS AirTaxi_Piston,
SUM(IF(SCCDescription="Air Taxi, Turbine",LTO, 0))*2 AS AirTaxi_Turbine,
SUM(IF(SCCDescription='Military', LTO, 0))*2 AS Military,
SUM(IF(SCCDescription='Commercial', LTO, 0))*2 AS Commercial
FROM FY21TCEQ_AirportEI.2017erg
Where EIS_Pollutant_ID = 'CO'
Group by FacilityID, Airport, COUNTY;
-- Get Total Ops from the ERG Data
FLUSH TABLES;
ALTER TABLE FY21TCEQ_AirportEI.2017ergOps
ADD COLUMN TotalOps float;

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.2017ergOps
SET TotalOps = GeneralAviation_Piston+GeneralAviation_Turbine+
AirTaxi_Piston+AirTaxi_Turbine+Military+Commercial;

-- Get Ops from 5010/ NFDC Data
FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.Texas_Facilities;
CREATE TABLE FY21TCEQ_AirportEI.Texas_Facilities
SELECT FacilityID, FacilityName, City, County,State,FacilityType, AirportStatusCode, ownership, Used, MedicalUse,Otherservices,
FuelTypes,MilitaryJointUse, SingleEngineGA, MultiEngineGA,JetEngineGA, HelicoptersGA,GlidersOperational,
 MilitaryOperational,  Ultralights, OperationsCommercial, OperationsCommuter, 
 OperationsAirTaxi, OperationsGALocal, OperationsGAItin,OperationsMilitary,Sum(OperationsCommuter+OperationsCommercial+ 
OperationsAirTaxi+ OperationsGALocal+ OperationsGAItin+OperationsMilitary) as TotalOps 
FROM FY21TCEQ_AirportEI.TX_AllAirports
where AirportStatusCode = 'O'
group by FacilityID;


-- Add placeholder columns for Ops and Taxi in and out times from different datasets.
ALTER TABLE FY21TCEQ_AirportEI.Texas_Facilities
ADD COLUMN 2017ERGOps float,
ADD COLUMN 2019TAFOps float,
ADD COLUMN 2020TAFOps float,
ADD COLUMN 2019tfmscOps float,
ADD COLUMN 2019Sum_tfmscOps float,
ADD COLUMN 2020tfmscOps float,
ADD COLUMN 2020Sum_tfmscOps float,
ADD COLUMN AirnavOps float,
ADD COLUMN Airnav_Repyear Int,
ADD COLUMN 2019TTIAnnOps float,
ADD COLUMN 2019TTISumOps float,
ADD COLUMN 2020TTIAnnOps float,
ADD COLUMN 2020TTISumOps float,
ADD COLUMN TxDOTGroup char (5),
ADD COLUMN FacilityGroup char(20),
ADD Column FarmorRanch char(5),
ADD COLUMN 2019AnnAvg_Taxiouttime float,
ADD COLUMN 2019AnnAvg_Taxiintime float,
ADD COLUMN 2019SumAvg_Taxiouttime float,
ADD COLUMN 2019SumAvg_Taxiintime float,
ADD COLUMN 2020AnnAvg_Taxiouttime float,
ADD COLUMN 2020AnnAvg_Taxiintime float,
ADD COLUMN 2020SumAvg_Taxiouttime float,
ADD COLUMN 2020SumAvg_Taxiintime float;

CREATE INDEX IF NOT EXISTS tx_fa_idx ON FY21TCEQ_AirportEI.Texas_Facilities (FacilityID);
CREATE INDEX IF NOT EXISTS taf_45 ON FY21TCEQ_AirportEI.TAF1990_2045 (FacilityID);
-- Add TAF 2019 Data
flush tables;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities a
JOIN FY21TCEQ_AirportEI.TAF1990_2045  b ON
a.FacilityID = b.FacilityID 
SET a.2019TAFOps = b.T_AOPS where b.SYSYEAR = '2019';

-- Add TAF 2020 Data
flush tables;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities a
JOIN FY21TCEQ_AirportEI.TAF1990_2045  b ON
a.FacilityID = b.FacilityID 
SET a.2020TAFOps = b.T_AOPS where b.SYSYEAR = '2020';

-- Add 2017 ERG Ops Data
flush tables;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities a
JOIN FY21TCEQ_AirportEI.2017ERGOps  b ON
a.FacilityID = b.FacilityID  
SET a.2017ERGOps = b.TotalOps;

-- Add 2019 TFMSC Ops
FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities r
JOIN (
	SELECT FacilityID
		,sum(TotalOps) AS TotalOps
	FROM FY21TCEQ_AirportEI.FAA_2019TFMSC
	group by FacilityID
	) AS grp ON grp.FacilityID = r.FacilityID
SET r.2019tfmscOps = grp.TotalOps;

-- Add 2019 TFMSC Summer Data
FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities r
JOIN (
	SELECT FacilityID
		,sum(TotalOps) AS TotalOps
	FROM FY21TCEQ_AirportEI.FAA_2019TFMSC
	where season = 'Summer'
	group by FacilityID
	) AS grp ON grp.FacilityID = r.FacilityID

SET r.2019Sum_tfmscOps = grp.TotalOps;

-- Add 2020 TFMSC Data
FLUSH TABLES;

UPDATE FY21TCEQ_AirportEI.Texas_Facilities r
JOIN (
	SELECT FacilityID
		,sum(TotalOps) AS TotalOps
	FROM FY21TCEQ_AirportEI.FAA_2020TFMSC
	group by FacilityID
	) AS grp ON grp.FacilityID = r.FacilityID

SET r.2020tfmscOps = grp.TotalOps;

-- Add 2020 TFMSC Summer Data
FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities r
JOIN (
	SELECT FacilityID
		,sum(TotalOps) AS TotalOps
	FROM FY21TCEQ_AirportEI.FAA_2020TFMSC
	where season = 'Summer'
	group by FacilityID
	) AS grp ON grp.FacilityID = r.FacilityID

SET r.2020Sum_tfmscOps = grp.TotalOps;

FLUSH TABLES;

-- Add AirNav Data
UPDATE FY21TCEQ_AirportEI.Texas_Facilities r
JOIN (
	SELECT FacilityID
		,Operations_Year
		,sum(Annual_Operations) AS TotalOps
	FROM FY21TCEQ_AirportEI.airnav
	group by FacilityID
	) AS grp ON grp.FacilityID = r.FacilityID

SET r.AirnavOps = grp.TotalOps
	,r.Airnav_Repyear = grp.Operations_Year;

-- Add TTI Data	
FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities a
JOIN FY21TCEQ_AirportEI.ttiops b On b.FacilityID = a.FacilityID
SET a.2019TTIAnnOps = b.2019TTIOps
	,a.2019TTISumOps = b.2019TTISumOps
	,a.2020TTIAnnOps = b.2020TTIops
	,a.2020TTISumOps = b.2020TTISumOps;
	
-- Add 2019 Avg Annual Taxi out Time
FLUSH TABLES;

UPDATE FY21TCEQ_AirportEI.Texas_Facilities r
JOIN (
	SELECT FacilityID
		,Avg(Avg_Taxiouttime) AS Avg_Taxiouttime
	FROM FY21TCEQ_AirportEI.faa_2019taxitime
	where Departures_Metrics > 0
	group by FacilityID
	) AS grp ON grp.FacilityID = r.FacilityID

SET r.2019AnnAvg_Taxiouttime = grp.Avg_Taxiouttime;

-- Add 2019 Avg Annual Taxi in Time
FLUSH TABLES;

UPDATE FY21TCEQ_AirportEI.Texas_Facilities r
JOIN (
	SELECT FacilityID
		,Avg(Avg_Taxiintime) AS Avg_Taxiintime
	FROM FY21TCEQ_AirportEI.faa_2019taxitime
	where Arrivals_metrics > 0
	group by FacilityID
	) AS grp ON grp.FacilityID = r.FacilityID

SET r.2019AnnAvg_Taxiintime = grp.Avg_Taxiintime;

-- Add 2020 Avg Annual Taxi out Time
FLUSH TABLES;

UPDATE FY21TCEQ_AirportEI.Texas_Facilities r
JOIN (
	SELECT FacilityID
		,Avg(Avg_Taxiouttime) AS Avg_Taxiouttime
	FROM FY21TCEQ_AirportEI.faa_2020taxitime
	where Departures_Metrics > 0
	group by FacilityID
	) AS grp ON grp.FacilityID = r.FacilityID

SET r.2020AnnAvg_Taxiouttime = grp.Avg_Taxiouttime;

-- Add 2020 Avg Annual Taxi in Time
FLUSH TABLES;

UPDATE FY21TCEQ_AirportEI.Texas_Facilities r
JOIN (
	SELECT FacilityID
		,Avg(Avg_Taxiintime) AS Avg_Taxiintime
	FROM FY21TCEQ_AirportEI.faa_2020taxitime
	where Arrivals_metrics > 0
	group by FacilityID
	) AS grp ON grp.FacilityID = r.FacilityID

SET r.2020AnnAvg_Taxiintime = grp.Avg_Taxiintime;

-- Add 2019 Avg Summer Taxi out Time
FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities r
JOIN (
	SELECT FacilityID
		,Avg(Avg_Taxiouttime) AS Avg_Taxiouttime
	FROM FY21TCEQ_AirportEI.faa_2019taxitime
	where Departures_Metrics > 0
		and season = 'Summer'
	group by FacilityID
	) AS grp ON grp.FacilityID = r.FacilityID

SET r.2019SumAvg_Taxiouttime = grp.Avg_Taxiouttime;

-- Add 2019 Avg Summer Taxi in Time
FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities r
JOIN (
	SELECT FacilityID
		,Avg(Avg_Taxiintime) AS Avg_Taxiintime
	FROM FY21TCEQ_AirportEI.faa_2019taxitime
	where Arrivals_metrics > 0
		and season = 'Summer'
	group by FacilityID
	) AS grp ON grp.FacilityID = r.FacilityID

SET r.2019SumAvg_Taxiintime = grp.Avg_Taxiintime;

-- Add 2020 Avg Summer Taxi Out Time
FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities r
JOIN (
	SELECT FacilityID
		,Avg(Avg_Taxiouttime) AS Avg_Taxiouttime
	FROM FY21TCEQ_AirportEI.faa_2020taxitime
	where Departures_Metrics > 0
		and season = 'Summer'
	group by FacilityID
	) AS grp ON grp.FacilityID = r.FacilityID

SET r.2020SumAvg_Taxiouttime = grp.Avg_Taxiouttime;

-- Add 2020 Avg Summer Taxi In Time
FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities r
JOIN (
	SELECT FacilityID
		,Avg(Avg_Taxiintime) AS Avg_Taxiintime
	FROM FY21TCEQ_AirportEI.faa_2020taxitime
	where Arrivals_metrics > 0
		and season = 'Summer'
	group by FacilityID
	) AS grp ON grp.FacilityID = r.FacilityID

SET r.2020SumAvg_Taxiintime = grp.Avg_Taxiintime;

-- Add TxDOT Facility Type
flush tables;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities a
JOIN FY21TCEQ_AirportEI.TxDOT_PlanningAirports  b ON
a.FacilityID = b.FacilityID 
SET a.TxDOTGroup = b.TxDOT;

-- Asigning Facility Type 
-- *********************************************************
-- Use Facility type, Ownership, Used, Medical Use, and Other services:
FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities
SET FacilityGroup = 'Other_PU_Airports' where Used ='PU' and FacilityType not in ('HELIPORT');

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities
SET FacilityGroup = 'Other_PU_Heliports' where Used ='PU' and FacilityType in ('HELIPORT');

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities
SET FacilityGroup = 'Other_PR_Airports' where Used ='PR' and FacilityType not in ('HELIPORT');

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities
SET FacilityGroup = 'Other_PR_Heliports' where Used ='PR' and FacilityType in ('HELIPORT');

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities
SET FacilityGroup = 'Farm/Ranch' where FacilityName like '%Farm%';

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities
SET FacilityGroup = 'Farm/Ranch' where FacilityName like '%Ranch%';

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities
SET FacilityGroup = 'Farm/Ranch' where Otherservices = 'AGRI';

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities
SET FacilityGroup = 'Medical' where MedicalUse = 'Y';

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities
SET FacilityGroup = 'Military' where ownership in( 'MA','MR','MN');

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities
SET FacilityGroup = 'TASP' where TxDOTGroup ='TASP';

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities
SET FacilityGroup = 'Reliever' where FacilityID in ('ADS','AFW','AXH','CXO','DTO','DWH','EFD','FTW','FWS','GKY','GLS','GPM','GTU','HQZ',
'HYI','IWS','LBX','LNC','LVJ','RBD','SGR','SKF','SSF','T41','TKI');

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.Texas_Facilities
SET FacilityGroup = 'Commercial' where FacilityID in ('ABI','ACT','AMA','AUS','BPT','BRO','CLL','CRP','DAL','DFW','DRT','ELP','GGG','HOU',
'HRL','IAH','ILE','LBB','LRD','MAF','MFE','SAT','SJT','SPS','TYR','VCT');