# Databricks notebook source
# DBTITLE 1,Import libraries
import requests
import io
import ssl
import pandas as pd
import csv
import numpy as np
import re
from datetime import datetime
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DateType

# COMMAND ----------

# DBTITLE 1,Download property price register
ssl._create_default_https_context = ssl._create_unverified_context

url = 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/RIQ02/CSV/1.0/en'
data = requests.get(url).content

# COMMAND ----------

# DBTITLE 1,Load property price register data to pand data frame
propertyData = pd.read_csv(io.StringIO(data.decode('utf-8')),
                           quoting=csv.QUOTE_ALL)

# COMMAND ----------

# DBTITLE 1,Cleanup data
propertyData.rename(columns={"TLIST(Q1)": "NQuarter",
                             "VALUE": "rent",
                             "Number of Bedrooms": "beds",
                             "Property Type": "propertyType"
                            }, inplace=True)
del propertyData['STATISTIC']
del propertyData['Statistic']
del propertyData['Quarter']
del propertyData['C02970V03592']
del propertyData['C02969V03591']
del propertyData['C03004V03625']
del propertyData['UNIT']

propertyData.drop(propertyData[pd.isna(propertyData.rent)].index, inplace=True)

# COMMAND ----------

# DBTITLE 1,Delete data older than 2015
propertyData.drop(propertyData[propertyData.NQuarter < 20151].index, inplace=True)

# COMMAND ----------

# DBTITLE 1,Compute town from address
def find_town(loctn):
  locList = loctn.split(',')
  locLen  = len(locList)
  if locLen < 2:
    town = loctn
  elif locList[locLen-1].strip().find("Dublin ") == 0 :
    town = locList[locLen-1]
  else:
    town = locList[0]
  return town.strip()

def find_county(loctn):
  locList = loctn.split(',')
  locLen  = len(locList)
  if locLen > 1:
    county = locList[locLen-1].strip()
  else:
    county = locList[0].strip()

  return re.sub(" .*$","",county)

def quarterDate(nQaurter):
  snQaurter = str(nQaurter)
  return pd.to_datetime(snQaurter[0:4] +'0' + snQaurter[4:5] + '01', format='%Y%m%d', errors='ignore')

propertyData["town"] = propertyData["Location"].apply(lambda x: find_town(x))
propertyData["county"] = propertyData["Location"].apply(lambda x: find_county(x))
propertyData["quarterDate"] = propertyData["NQuarter"].apply(lambda x: quarterDate(x))

# COMMAND ----------

# DBTITLE 1,Write data to databricks table
schema = StructType([
    StructField("NQuater",IntegerType(),True),
    StructField("beds",StringType(),True),
    StructField("propertyType",StringType(),True),
    StructField("Location",StringType(),True),
    StructField("rent",DoubleType(),True),
    StructField("town",StringType(),True),
    StructField("county",StringType(),True),
    StructField("quarterDate",DateType(),True)
])

propertyDataSDF = spark.createDataFrame(propertyData, schema)
propertyDataSDF.write.mode("overwrite").partitionBy("county").saveAsTable("property_rent_register")