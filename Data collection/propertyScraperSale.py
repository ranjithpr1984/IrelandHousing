# Databricks notebook source
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import re
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import LongType
from pyspark.sql.types import IntegerType

def getText(xmlObj):
  return None if xmlObj == None else xmlObj.text

pageSize = 20
 
homeLink = 'https://www.property.ie/property-for-sale/'

counties = ["carlow","cavan","clare","cork","donegal","dublin","galway","kerry","kildare","kilkenny","laois","leitrim","limerick","longford","louth","mayo","meath","monaghan","offaly","roscommon","sligo","tipperary","waterford","westmeath","wexford","wicklow"]

schema = StructType([
    StructField("price",LongType(),True),
    StructField("town",StringType(),True),
    StructField("county",StringType(),True),
    StructField("beds",StringType(),True),
    StructField("baths",StringType(),True),
    StructField("propertyType",StringType(),True),
    StructField("address",StringType(),True),
    StructField("Website",StringType(),True)
])

propertyData = spark.createDataFrame(sc.emptyRDD(), schema)

for county in counties:
  offset = 1
  propertyData = list()

  while (True):
    pageLink = homeLink + county + '/price_international_rental-onceoff_standard/beds_3/p_' + str(offset) + '/'
    print(pageLink)
    req = Request(pageLink, headers={'User-Agent': 'Mozilla/5.0'})
    page = urlopen(req).read()
    soup = BeautifulSoup(page, 'html.parser')
    counter = 0
    offset += 1
    for propertyDetails in soup.findAll('div', attrs={'class': 'search_result'}):
      counter+= 1
      priceField = getText(propertyDetails.find('h3')).strip()
      price = re.sub("[^0-9]", "", priceField)
      if(len(price) == 0):
        continue;
      address = re.sub("^\d+\.\s+", "",getText(propertyDetails.find('div', attrs={'class': 'sresult_address'})).strip()).strip()
      address_list = address.strip().split(',')
      address_len = len(address_list)
      l_county = county.capitalize()
      if(address_len < 3):
        continue
      if address_list[address_len-2].replace(" ","").find("Co." + l_county) == 0 :
        l_town = address_list[address_len-3].strip()
      elif address_list[address_len-1].replace(" ","").find("Co." + l_county) == 0 :
        l_town = address_list[address_len-2].strip()
      elif address_list[address_len-2].strip() == l_county :
        l_town = address_list[address_len-3].strip()
      elif address_list[address_len-1].strip() == l_county  :
        l_town = address_list[address_len-2].strip()
      elif(address_list[address_len-1].find(l_county) > -1):
        l_town = address_list[address_len-1].strip()
      elif(address_list[address_len-2].find(l_county) > -1):
        l_town = address_list[address_len-2].strip()
      else:
        continue

      OtherData = getText(propertyDetails.find('h4')).strip().split(',')
      beds = None
      baths = None
      propertyType = None
      for i in range(len(OtherData)):
        if OtherData[i].find("Bedrooms") >  -1:
          beds = re.sub("[^0-9]", "", OtherData[i])
        elif OtherData[i].find("Bathrooms") >  -1:
          baths = re.sub("[^0-9]", "", OtherData[i])
        else: 
          propertyType = OtherData[i].replace('For Sale','').replace(' House','').strip()

      if (propertyType == None) | (beds == None) :
        continue

      if (propertyType == 'Site') | (propertyType == 'Bungalow') | (propertyType == '') | (int(beds) > 4) :
        continue

      propertyData.append([int(price), l_town, l_county, beds, baths, propertyType, address, 'property.ie'])
    
    if(counter < pageSize):
      break;

  propertyDataDF = spark.createDataFrame(propertyData, schema)
  propertyDataDF.write.mode("append").partitionBy("county").saveAsTable("default.property_price_data")