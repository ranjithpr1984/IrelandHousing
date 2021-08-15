# Databricks notebook source
# DBTITLE 1,Import libraries
import urllib.request
import zipfile
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

url = 'https://propertypriceregister.ie/website/npsra/ppr/npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip'
fname = '/tmp/PPR-ALL.csv'
filehandle, _ = urllib.request.urlretrieve(url)
zip_file_object = zipfile.ZipFile(filehandle, 'r')
first_file = zip_file_object.namelist()[0]

# COMMAND ----------

# DBTITLE 1,Load property price register data to pand data frame
dateparse = lambda x: datetime.strptime(x, '%d/%m/%Y')

propertyData = pd.read_csv(zip_file_object.open(first_file),encoding = "ISO-8859-1",
                           quoting=csv.QUOTE_ALL, parse_dates=['Date of Sale (dd/mm/yyyy)'],
                           date_parser=dateparse)
del propertyData['Property Size Description']
propertyData.rename(columns={"Date of Sale (dd/mm/yyyy)": "SaleDate", 
                             "Postal Code": "town", "Price ()":"price", 
                             "Description of Property" : "propertyType", 
                             "VAT Exclusive" : "VATExclusive",
                             "Not Full Market Price" : "NotFullMarketPrice",
                             "Address" : "address",
                             "County" : "county"
                            }, inplace=True)

# COMMAND ----------

# DBTITLE 1,Cleanup data
propertyData.drop(propertyData[propertyData.NotFullMarketPrice == "Yes"].index, inplace=True)
del propertyData['NotFullMarketPrice']
propertyData = propertyData.convert_dtypes()
propertyData["price"] = propertyData["price"].apply(lambda x: pd.to_numeric(x.replace(',', '').replace('\x80', '')))
propertyData["address"] = propertyData["address"].apply(lambda x: x.title())
propertyData.info()

# COMMAND ----------

# DBTITLE 1,Delete data older than 2015
l_min_date = pd.to_datetime('20150101', format='%Y%m%d', errors='ignore')
propertyData = propertyData[propertyData['SaleDate'] >= l_min_date ]

# COMMAND ----------

# DBTITLE 1,Add VAT for VAT excluded sales
vat_excl_idx = propertyData["VATExclusive"] == "Yes"
propertyData.loc[vat_excl_idx, 'price'] = propertyData.loc[vat_excl_idx, 'price'] * 1.135
del propertyData['VATExclusive']

# COMMAND ----------

# DBTITLE 1,Compute town from address
for i in propertyData.index:
  l_town = propertyData.at[i, "town"]
#  if l_town is not np.nan :
  if pd.notna(l_town) :
    continue
  
  address = propertyData.at[i, "address"].strip()
  address_list = address.split(',')
  address_len = len(address_list)
  if address_len < 2 :
    continue
    
  c_county = propertyData.at[i, "county"].capitalize()
  l_county = propertyData.at[i, "county"].lower()

  last_part = re.sub("[ .]","",address_list[address_len-1]).capitalize()
  slast_part = re.sub("[ .]","",address_list[address_len-2]).capitalize()

  if (slast_part == c_county) & (last_part == c_county):
    l_town = address_list[address_len-2]
  elif (slast_part == c_county) & (address_len > 2):
    l_town = address_list[address_len-3]
  elif (slast_part.find("Dublinrd")) == 0 | (slast_part.find("Dublinroad") == 0) :
    l_town = address_list[address_len-1]
  elif slast_part.find("Dublin") == 0 :
    l_town = address_list[address_len-2]
  elif (slast_part.find("Co" + l_county) == 0) & (address_len > 2) :
    l_town = address_list[address_len-3]
  elif (slast_part.find("County" + l_county) == 0) & (address_len > 2) :
    l_town = address_list[address_len-3]
  elif last_part == c_county :
    l_town = address_list[address_len-2]
  elif (last_part.find("Dublinrd")) == 0 | (last_part.find("Dublinroad") == 0) :
    l_town = address_list[address_len-2]
  elif last_part.find("Dublin") == 0 :
    l_town = address_list[address_len-1]
  elif last_part.find("Co" + l_county) == 0 :
    l_town = address_list[address_len-2]
  elif last_part.find("County" + l_county) == 0 :
    l_town = address_list[address_len-2]
  else:
    l_town = address_list[address_len-1]

  propertyData.at[i, "town"] = l_town.strip().title()

# COMMAND ----------

propertyData["saleMonth"] = propertyData["SaleDate"].dt.to_period('M').astype("string")

# COMMAND ----------

# DBTITLE 1,Write data to databricks table
schema = StructType([
    StructField("SaleDate",DateType(),True),
    StructField("address",StringType(),True),
    StructField("town",StringType(),True),
    StructField("county",StringType(),True),
    StructField("price",DoubleType(),True),
    StructField("propertyType",StringType(),True),
    StructField("saleMonth",StringType(),True)
])

propertyDataSDF = spark.createDataFrame(propertyData, schema)

propertyDataSDF.write.mode("overwrite").partitionBy("county").saveAsTable("property_price_register")


# COMMAND ----------

# MAGIC %sql select * from property_price_register