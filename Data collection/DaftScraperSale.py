# Databricks notebook source
from bs4 import BeautifulSoup
import urllib.request
import re
import time
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import LongType
from pyspark.sql.types import IntegerType


def getText(xmlObj):
  return None if xmlObj == None else xmlObj.text


pageSize = 20
 
homeLink = 'https://www.daft.ie/property-for-sale/'

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
  offset = 0
  propertyData = list()

  while (True):
    pageLink = homeLink + county + '?numBeds_from=3&numBeds_to=4&pageSize=20&propertyType=detached-houses&propertyType=semi-detached-houses&propertyType=terraced-houses&propertyType=end-of-terrace-houses&propertyType=apartments&propertyType=duplexes&propertyType=townhouses&from=' + str(offset)
    print(pageLink)
    page = urllib.request.urlopen(pageLink)
    soup = BeautifulSoup(page, 'html.parser')
    counter = 0
    for propertyDetails in soup.findAll('li', attrs={'class': re.compile("SearchPage__Result.*")}):
      counter += 1
      address = getText(propertyDetails.find('p', attrs={'data-testid': 'address'})).strip()
      if(address == None):
        continue;
      address_list = address.split(',')
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

      subCounter = 0
      for subPropertyDetails in propertyDetails.findAll('div', attrs={'class': re.compile("SubUnit__CardInfoWrapper.*")}):
        subCounter += 1
        price = re.sub("[^0-9]", "",getText(subPropertyDetails.find('p', attrs={'data-testid': 'sub-title'})))
        if(len(price) == 0):
          continue
        OtherData = getText(subPropertyDetails.find('div', attrs={'data-testid': 'sub-line-2-info'})).split('·')
        beds = re.sub("[^0-9]","",OtherData[0])
        baths = re.sub("[^0-9]","",OtherData[1])
        propertyType = OtherData[2].replace("Virtual Tour","").replace('Semi-D','Semi-Detached').strip()
        if propertyType == 'Terrace':
          propertyType = 'Terraced'

        if(beds == None):
          continue

        propertyData.append([int(price), l_town, l_county, beds, baths, propertyType, address, 'daft.ie'])

      if subCounter > 0:
        continue

      price = re.sub("[^0-9]", "",getText(propertyDetails.find('div', attrs={'data-testid': 'price'})))
      if(len(price) == 0):
        continue
      beds = getText(propertyDetails.find('p', attrs={'data-testid': 'beds'}))
      baths = getText(propertyDetails.find('p', attrs={'data-testid': 'baths'}))
      propertyType = getText(propertyDetails.find('p', attrs={'data-testid': 'property-type'})).replace("Virtual Tour","").replace('Semi-D','Semi-Detached').strip()
      if propertyType == 'Terrace':
        propertyType = 'Terraced'

      if(beds == None):
        continue;

      beds = re.sub("[^0-9]", "", beds)
      if(baths != None):
        baths = re.sub("[^0-9]", "", baths)
 
      propertyData.append([int(price), l_town, l_county, beds, baths, propertyType, address, 'daft.ie'])
    
    if(counter < pageSize):
      break;
    offset += pageSize

  propertyDataDF = spark.createDataFrame(propertyData, schema)
  propertyDataDF.write.mode("append").partitionBy("county").saveAsTable("default.property_price_data")
  time.sleep(10)