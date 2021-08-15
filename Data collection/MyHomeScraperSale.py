# Databricks notebook source
from bs4 import BeautifulSoup
#import urllib.request
from urllib.request import Request, urlopen
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
 
homeLink = 'https://www.myhome.ie/residential/'

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
    pageLink = homeLink + county + '/property-for-sale?types=36|37|38|39|45|47|48&minbeds=3&maxbeds=4&page=' + str(offset)
    print(pageLink)
    req = Request(pageLink, headers={'User-Agent': 'Mozilla/5.0'})
    page = urlopen(req).read()
    #page = urllib.request.urlopen(pageLink)
    soup = BeautifulSoup(page, 'html.parser')
    counter = 0
    offset += 1
    for propertyDetails in soup.findAll('div', attrs={'class': re.compile("PropertyListingCard__PropertyInfo.*")}):
      counter+= 1
      propertyType = beds = baths = None
      priceField = getText(propertyDetails.find('div', attrs={'class': re.compile("PropertyListingCard__Price.*")})).strip()
      price = re.sub("[^0-9]", "", priceField)
      if(len(price) == 0):
        continue
      address = getText(propertyDetails.find('a', attrs={'class': re.compile("PropertyListingCard__Address.*")})).strip()
      address_list = address.strip().split(',')
      address_len = len(address_list)
      l_county = county.capitalize()
      if(address_len < 3):
        continue
      if re.sub("[ .]","",address_list[address_len-2]).find("Co" + l_county) == 0 :
        l_town = address_list[address_len-3].strip()
      elif re.sub("[ .]","",address_list[address_len-1]).find("Co" + l_county) == 0 :
        l_town = address_list[address_len-2].strip()
      elif re.sub("[ .]","",address_list[address_len-2]).find("County" + l_county) == 0 :
        l_town = address_list[address_len-3].strip()
      elif re.sub("[ .]","",address_list[address_len-1]).find("County" + l_county) == 0 :
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

      for OtherData in propertyDetails.findAll('span', attrs={'class': 'PropertyInfoStrip__Detail PropertyInfoStrip__Detail--dark ng-star-inserted'}):
        if OtherData.find('app-mh-icon') == None:
          continue
        data = OtherData.text
        icon = OtherData.find('app-mh-icon')['icon']
        if icon == 'bed':
          beds = re.sub("[^0-9]", "", data)
        elif icon == 'bath':
          baths = re.sub("[^0-9]", "", data)
        elif icon == 'home':
          propertyType = data.replace(' House','').strip()

      propertyData.append([int(price), l_town, l_county, beds, baths, propertyType, address,'myhome.ie'])

    if(counter < pageSize):
      break

  propertyDataDF = spark.createDataFrame(propertyData, schema)
  propertyDataDF.write.mode("append").partitionBy("county").saveAsTable("default.property_price_data")
  time.sleep(30)