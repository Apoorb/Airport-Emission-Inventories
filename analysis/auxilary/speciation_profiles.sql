/****** Script for SelectTopNRows command from SSMS  ******/
/* dbo.STN_MASSFRAC
This table contains all of the speciation profile mass fractions of TOG mass.*/
SELECT A.[PROF_ID]
	,A.[POL_ID]
	,[SPECIATEID]
	,B.[NAME] AS [Prof_nm]
	,C.[NAME] AS [Pol_nm]
	,[MASSFRAC]
	,[CAS_NUM]
	,[CAA_HAP]
	,[IRIS_HAP]
	,[HC]
	,[VOC]
FROM [FLEET].[dbo].[STN_MASSFRAC] AS A
JOIN [FLEET].[dbo].[STN_SPECPROF] AS B ON A.[PROF_ID] = B.[PROF_ID]
JOIN [FLEET].[dbo].[STN_SPEC_HC] AS C ON A.[POL_ID] = C.[POL_ID]
WHERE B.[NAME] IN ('Aircraft: Piston', 'Aircraft: Turbine and APU', 'GSE: Diesel', 'GSE: Gasoline, LPG and CNG');