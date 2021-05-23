SET SQL_SAFE_UPDATES = 0;

FLUSH TABLES;
Drop Schema if exists FY21TCEQ_AirportEI;
Create Schema FY21TCEQ_AirportEI;

-- Creating Tables and loading data to the tables

FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.TAF9045;
create table FY21TCEQ_AirportEI.TAF9045(
LOC_ID char(10),
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
ITN_MIL Int,
T_ITN Int,
LOC_GA Int,
LOC_MIL Int,
T_LOC Int,
T_AOPS Int,
T_TROPS Int,
TOT_BA Int);

FLUSH TABLES;
LOAD DATA LOCAL INFILE 'D:/Projects/Airport/Data/FAA_1990_2040_TAF.csv' INTO
TABLE FY21TCEQ_AirportEI.TAF9045
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\r\n'
ignore 1 lines;

FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.FAA_TXAirports;
create table FY21TCEQ_AirportEI.FAA_TXAirports(
SiteNumber char(10),
FacilityType char(20),
LocationID char(10),
EffectiveDate char(12),
Region char(3),
DistrictOffice char(5),
State char(2),
StateName char(10),
County char(25),
CountyState char(2),
City char(25),
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
LOAD DATA LOCAL INFILE 'D:/Projects/Airport/Data/FAA_NfdcFacilities.csv' INTO
TABLE FY21TCEQ_AirportEI.FAA_TXAirports
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\r\n'
ignore 1 lines ;

FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.FAA_2019taxitime;
create table FY21TCEQ_AirportEI.FAA_2019taxitime(
Date char(20),
Year Int,
Month char(15),
Facility char(10),
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
LOAD DATA LOCAL INFILE 'D:/Projects/Airport/Data/FAA_2019TX_Taxitime.csv' INTO
TABLE FY21TCEQ_AirportEI.FAA_2019taxitime
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\n'
ignore 1 lines;

FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.FAA_2019TFMSC;
create table FY21TCEQ_AirportEI.FAA_2019TFMSC(
yearid int,
monthname char(25),
LocationID char(5),
Airport char(25),
FlightType Char(25),
UserClass Char(25),
WeightClass Char(50),
PhysicalClass Char(25),
AircraftID Char(5),
AircraftType Char(50),
BusinessJet Char(10),
RegionalJet Char(10),
BusinessAviation Char(10),
AirplaneApproachCategory Char(10),	
AirplaneDesignGroup Char(10),
TaxiwayDesign Char(10),
Departures Int,
Arrivals Int,
TotalOps Int,
DepartureSeats Int,
AverageDepatureSeats Int,
ArrivalSeats Int,
AverageArrivalSeats Int);

FLUSH TABLES;
LOAD DATA LOCAL INFILE 'D:/Projects/Airport/Data/FAA_2019_Texas_TFMSC.csv' INTO
TABLE FY21TCEQ_AirportEI.FAA_2019TFMSC
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\r\n'
ignore 1 lines;

FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.TxDOT_PlanningAirports;
create table FY21TCEQ_AirportEI.TxDOT_PlanningAirports(
Loc_ID char(10),
Region_ID Int,
Airport_Name char(100),
City char(50));

FLUSH TABLES;
LOAD DATA LOCAL INFILE 'D:/Projects/Airport/Data/TxDOT_Airports.csv' INTO
TABLE FY21TCEQ_AirportEI.TxDOT_PlanningAirports
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\r\n'
ignore 1 lines;

FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.ERG2017;
create table FY21TCEQ_AirportEI.ERG2017(
StateFacilityIdentifier char(10), 
Airport char(50),
COUNTY char(25),
FIP Int,
SCC Char(25),
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
LOAD DATA LOCAL INFILE 'D:/Projects/Airport/Data/ERG_2017.csv' INTO
TABLE FY21TCEQ_AirportEI.ERG2017
FIELDS TERMINATED BY ','
Optionally Enclosed By '"'
LINES TERMINATED BY '\r\n'
ignore 1 lines;

FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.ERG2017LTOSummary;
CREATE TABLE FY21TCEQ_AirportEI.ERG2017LTOSummary
SELECT StateFacilityIdentifier, Airport, COUNTY, 
SUM(IF(SCCDescription="General Aviation, Piston", LTO, 0)) AS GeneralAviation_Piston,
SUM(IF(SCCDescription="General Aviation, Turbine", LTO, 0)) AS GeneralAviation_Turbine,
SUM(IF(SCCDescription="Air Taxi, Piston", LTO, 0)) AS AirTaxi_Piston,
SUM(IF(SCCDescription="Air Taxi, Turbine",LTO, 0)) AS AirTaxi_Turbine,
SUM(IF(SCCDescription='Military', LTO, 0)) AS Military,
SUM(IF(SCCDescription='Commercial', LTO, 0)) AS Commercial
FROM FY21TCEQ_AirportEI.ERG2017
Where EIS_Pollutant_ID = 'CO'
Group by StateFacilityIdentifier, Airport, COUNTY;

FLUSH TABLES;
ALTER TABLE FY21TCEQ_AirportEI.ERG2017LTOSummary
ADD COLUMN TotalLTO float;

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.ERG2017LTOSummary
SET TotalLTO = GeneralAviation_Piston+GeneralAviation_Turbine+
AirTaxi_Piston+AirTaxi_Turbine+Military+Commercial;

FLUSH TABLES;
Drop table if exists FY21TCEQ_AirportEI.TXOpsAirports;
CREATE TABLE FY21TCEQ_AirportEI.TXOpsAirports
SELECT LocationID, City, County,State, ARPLatitudeS as latitute, ARPLongitudeS as longitute, FacilityType, FacilityName, AirportStatusCode, 
ownership, used, MedicalUse,otherservices,FuelTypes
SingleEngineGA, MultiEngineGA,JetEngineGA, HelicoptersGA,GlidersOperational,
 MilitaryOperational,  Ultralights, OperationsCommercial, OperationsCommuter, 
 OperationsAirTaxi, OperationsGALocal, OperationsGAItin,OperationsMilitary 
FROM FY21TCEQ_AirportEI.FAA_TXAirports
where AirportStatusCode = 'O';


FLUSH TABLES;
ALTER TABLE FY21TCEQ_AirportEI.TXOpsAirports
ADD COLUMN TotalOps float,
ADD COLUMN 2017ERGOps float,
ADD COLUMN TxDOTPlanAirports char (1),
ADD Column FacilityGroup char(25);


FLUSH TABLES;
ALTER TABLE FY21TCEQ_AirportEI.TxDOT_PlanningAirports
ADD COLUMN AirportYN char(1);

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.TxDOT_PlanningAirports
SET AirportYN = 'Y';

flush tables;
UPDATE FY21TCEQ_AirportEI.TXOpsAirports a
JOIN FY21TCEQ_AirportEI.TxDOT_PlanningAirports  b ON
a.LocationID = b.Loc_ID 
SET a.TxDOTPlanAirports = b.AirportYN;

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.TXOpsAirports
SET TotalOps = (OperationsCommercial+ OperationsCommuter+
OperationsAirTaxi+ OperationsGALocal+ OperationsGAItin+
OperationsMilitary);

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.TXOpsAirports
SET FacilityGroup = 'Other_PU_Airports' where Used ='PU' and FacilityType not in ('HELIPORT');

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.TXOpsAirports
SET FacilityGroup = 'Other_PU_Airports' where Used ='PU' and FacilityType in ('HELIPORT');

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.TXOpsAirports
SET FacilityGroup = 'Other_PR_Airports' where Used ='PR' and FacilityType not in ('HELIPORT');

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.TXOpsAirports
SET FacilityGroup = 'Other_PR_Airports' where Used ='PR' and FacilityType in ('HELIPORT');

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.TXOpsAirports
SET FacilityGroup = 'Farm/Ranch' where FacilityName like '%Farm%';

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.TXOpsAirports
SET FacilityGroup = 'Farm/Ranch' where Otherservices = 'AGRI';

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.TXOpsAirports
SET FacilityGroup = 'Medical' where MedicalUse = 'Y';

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.TXOpsAirports
SET FacilityGroup = 'Military' where ownership in( 'MA','MR','MN');

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.TXOpsAirports
SET FacilityGroup = 'TASP' where TxDOTPlanAirports ='Y';

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.TXOpsAirports
SET FacilityGroup = 'Reliever' where LocationID in ('ADS','AFW','AXH','CXO','DTO','DWH','EFD','FTW','FWS','GKY','GLS','GPM','GTU','HQZ',
'HYI','IWS','LBX','LNC','LVJ','RBD','SGR','SKF','SSF','T41','TKI');

FLUSH TABLES;
UPDATE FY21TCEQ_AirportEI.TXOpsAirports
SET FacilityGroup = 'Commercial' where LocationID in ('ABI','ACT','AMA','AUS','BPT','BRO','CLL','CRP','DAL','DFW','DRT','ELP','GGG','HOU',
'HRL','IAH','ILE','LBB','LRD','MAF','MFE','SAT','SJT','SPS','TYR','VCT');


