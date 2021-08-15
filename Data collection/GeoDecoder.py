# Databricks notebook source
import requests

GOOGLE_API_KEY = 'AIzaSyDywwPvAcTTsEhH3Tz_ZlyRILcGDPBHhvA' 

def geocoder(address_or_zipcode):
  lat, lng = None, None
  api_key = GOOGLE_API_KEY
  base_url = "https://maps.googleapis.com/maps/api/geocode/json"
  endpoint = f"{base_url}?address={address_or_zipcode}&key={api_key}"
  r = requests.get(endpoint)
  if r.status_code not in range(200, 299):
    return None, None
  try:
    results = r.json()['results'][0]
    lat = results['geometry']['location']['lat']
    lng = results['geometry']['location']['lng']
  except:
    pass
  return lat, lng

# COMMAND ----------

Towns = spark.sql("select pd1.county,pd1.town from default.property_price_register as pd1  where (pd1.county, pd1.town) not in (select t.county,t.town from towns t)  group by pd1.county,pd1.town having count(1) > 50")

# COMMAND ----------

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType

schema = StructType([
    StructField("town",StringType(),True),
    StructField("county",StringType(),True),
    StructField("latitude",StringType(),True),
    StructField("longitude",StringType(),True),
])

Towns_new = list()

for row in Towns.rdd.collect():
  [lat, lng] = geocoder(row.town + ", County " + row.county)
  if(lat != None) & (lng != None):
    Towns_new.append([row.town, row.county, lat, lng])
  else:
    print(row.town + ", County " + row.county)

TownsSDF = spark.createDataFrame(Towns_new, schema)
TownsSDF.write.mode("append").saveAsTable("Towns")