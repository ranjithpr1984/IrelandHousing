-- Databricks notebook source
-- DBTITLE 1,MyHome sale scraper testing
select count(1) from property_price_data where website='myhome.ie';

-- COMMAND ----------

select * from property_price_data where website='myhome.ie' and
county='Donegal' and town='Letterkenny' and price in(135000,145000,125000);

-- COMMAND ----------

-- DBTITLE 1,Property.ie sale scraper testing
select count(1) from property_price_data where website='property.ie';

-- COMMAND ----------

select * from property_price_data where website='property.ie' and
county='Donegal' and town='Letterkenny' and price in(135000,145000,125000);

-- COMMAND ----------

-- DBTITLE 1,Daft sale scraper testing
select count(1) from property_price_data where website='daft.ie';

-- COMMAND ----------

select * from property_price_data where website='daft.ie' and
county='Donegal' and town='Letterkenny' and price in(265000,320000);

-- COMMAND ----------

-- DBTITLE 1,Daft rent scraper testing
select count(1) from property_rent_data where website='daft.ie';

-- COMMAND ----------

select * from property_rent_data where website='daft.ie' and
county='Donegal' and town='Letterkenny' and rent in(600,1000);

-- COMMAND ----------

-- DBTITLE 1,MyHome rent scraper testing
select count(1) from property_rent_data where website='myhome.ie';

-- COMMAND ----------

select * from property_rent_data where website='myhome.ie' and
county='Dublin' and town='Dublin 1' and rent in (2400)

-- COMMAND ----------

-- DBTITLE 1,Proerty.ie rent scraper testing
select count(1) from property_rent_data where website='property.ie';

-- COMMAND ----------

select * from property_rent_data where website='property.ie' and
county='Dublin' and town='Dublin 2' and rent in (4150,4000,4185)

-- COMMAND ----------

-- DBTITLE 1,Property price register min/max date
select min(SaleDate) min_sale_date, max(SaleDate) max_sale_date
from property_price_register

-- COMMAND ----------

-- DBTITLE 1,Property rent register after data extract
select min(NQuater) min_Quater, max(NQuater) max_Quater, count(1) County
from property_rent_register

-- COMMAND ----------

-- DBTITLE 1,Geo Decoding tesing
Select count(1) from towns

-- COMMAND ----------

select * from towns where county='Donegal' and town='Letterkenny';

-- COMMAND ----------

select price from property_price_data_final 
where county='Donegal' and town='Letterkenny' and beds='3'

-- COMMAND ----------

-- DBTITLE 1,Non pandamic property price prediction
select * from property_price_pred 
where county='Sligo'
and saleMonth between '2020-01' and '2021-08' 
order by saleMonth

-- COMMAND ----------

-- DBTITLE 1,Future property price prediction
select * from property_price_pred 
where county='Sligo' and saleMonth > '2021-08'

-- COMMAND ----------

-- DBTITLE 1,Non pandamic property rent prediction
select * from property_rent_pred 
where county='Sligo'
and NQuater between '20201' and '20213' 
order by NQuater

-- COMMAND ----------

-- DBTITLE 1,Future property rent prediction
select * from property_rent_pred 
where county='Sligo' and NQuater > '20213'
order by NQuater

-- COMMAND ----------

-- DBTITLE 1,EMI calculator testing
select * from property_price_data_final 
where county='Donegal' and price=250000

-- COMMAND ----------

-- DBTITLE 1,Extract property_price_data_final.csv
select price,p.town,p.county,propertyType,beds,latitude,longitude 
from property_price_data_final p
join towns t on t.county=p.county and t.town=p.town

-- COMMAND ----------

-- DBTITLE 1,Extract property_rent_data_final.csv
select rent,p.town,p.county,propertyType,beds,latitude,longitude 
from property_rent_data_final p
join towns t on t.county=p.county and t.town=p.town

-- COMMAND ----------

-- DBTITLE 1,Extract property_rent_emi.csv
select pra.*,t.latitude,t.longitude,ppa.emi
from property_rent_avg pra
join property_price_avg ppa on pra.county=ppa.county and pra.town=ppa.town and pra.beds=ppa.beds 
join towns t on t.county=pra.county and t.town=pra.town

-- COMMAND ----------

-- DBTITLE 1,Extract property_price_pred.csv
select * from property_price_pred

-- COMMAND ----------

-- DBTITLE 1,Extract property_rent_pred.csv
select * from property_rent_pred