/****** Script for SelectTopNRows command from SSMS  ******/
SELECT *
  FROM [FLEET].[dbo].[FLT_ANP_AIRPLANE_PROFILES]
  JOIN [FLEET].[dbo].[FLT_ANP_AIRPLANES]
  ON [FLEET].[dbo].[FLT_ANP_AIRPLANE_PROFILES].[ACFT_ID] = [FLEET].[dbo].[FLT_ANP_AIRPLANES].[ACFT_ID]
  JOIN [FLEET].[dbo].[FLT_EQUIPMENT]
  ON [FLEET].[dbo].[FLT_ANP_AIRPLANE_PROFILES].[ACFT_ID] = [FLEET].[dbo].[FLT_EQUIPMENT].[ANP_AIRPLANE_ID]
  JOIN  [FLEET].[dbo].[FLT_AIRFRAMES]
  ON [FLEET].[dbo].[FLT_EQUIPMENT].[AIRFRAME_ID] = [FLEET].[dbo].[FLT_AIRFRAMES].[AIRFRAME_ID] 
  JOIN  (SELECT [ENGINE_ID], [MODEL] AS [engine],[RATED_OUT],[UA_RWF_TO],[UA_RWF_CO],[UA_RWF_AP],[UA_RWF_ID] FROM [FLEET].[dbo].[FLT_ENGINES]) b
  ON [FLEET].[dbo].[FLT_EQUIPMENT].[ENGINE_ID] = b.[ENGINE_ID] 
  WHERE [MODEL] like ('%SMR100%') AND [engine] like ('%%') AND [PROF_ID1] = 'STANDARD' AND [OP_TYPE] = 'D'
  ORDER BY [PROF_ID2] DESC 
  ;

  -- 
  --WHERE [MODEL] = 'Airbus A319-100 Series' AND [engine] like ('%V2522-D%') AND [PROF_ID1] = 'STANDARD' AND [OP_TYPE] = 'D';
  -- AND [engine] = '01P02GE188'
