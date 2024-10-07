import pandas as pd
import PyUber
import csv
from datetime import datetime
import os

sql_opn = """
SELECT 
          eo.entity AS entity
         ,eo.operation AS operation
         ,Replace(Replace(Replace(Replace(Replace(Replace(o.oper_short_desc,',',';'),chr(9),' '),chr(10),' '),chr(13),' '),chr(34),''''),chr(7),' ') AS oper_short_desc
         ,Replace(Replace(Replace(Replace(Replace(Replace(o.oper_long_desc,',',';'),chr(9),' '),chr(10),' '),chr(13),' '),chr(34),''''),chr(7),' ') AS oper_long_desc
         ,o.dotprocess AS dotprocess
         ,o.cu_flag AS cu_flag
         ,o.cudesegeqpallowed AS cudesegeqpallowed
         ,o.rework_allowed AS rework_allowed
         ,o.module AS module
         ,eo.latest_flag AS entity_oper_latest_flag
FROM 
F_EntityOper eo
LEFT JOIN F_Operation O ON o.operation=eo.operation AND o.facility = eo.facility AND o.latest_version = 'Y'
WHERE
O.OPER_SHORT_DESC like 'L%' and (eo.ENTITY LIKE  'CAN%'
  OR eo.entity LIKE 'CIX%'
  OR eo.entity LIKE 'CNP%'
  OR eo.entity LIKE 'SBH%'
  OR eo.entity LIKE 'SCJ%'
  OR eo.entity LIKE 'SDJ%'
  OR eo.entity LIKE 'SLM%'
  OR eo.entity LIKE 'SNI%'
  OR eo.entity LIKE 'SNR%'
  OR eo.entity LIKE 'SNQ%'
  OR eo.entity LIKE 'STA%'
  OR eo.entity LIKE 'STB%'
  OR eo.entity LIKE 'STG%'
  OR eo.entity LIKE 'TBC%'
  OR eo.entity LIKE 'TCP%'
  OR eo.entity LIKE 'TCV%'
  OR eo.entity LIKE 'TCX%'
  OR eo.entity LIKE 'TNI%'
  OR eo.entity LIKE 'TTK%'
  OR eo.entity LIKE 'TZH%'
  OR eo.entity LIKE 'UTX%'
  )
   AND      eo.latest_flag = 'Y'
"""

sql_AM = """
SELECT AM_LDR_PATH, AM_LDR_MODELNAME, ENTITY, ROUTE, OPERATION, PRODUCT, PARAMETER_LIST FROM F_AM_F3
WHERE AM_LDR_PATH like '%Litho%' and PARAMETER_LIST like '%TRACK_RECIPE%'
"""

try:
    conn = PyUber.connect(datasource='F21_PROD_XEUS')
    df_opn = pd.read_sql(sql_opn, conn)
    df_resistAM = pd.read_sql(sql_AM, conn)
except:
    print('Cannot run SQL script - Consider connecting to VPN')

print(df_opn.head())
print(df_resistAM.head())   