-- Databricks notebook source
-- DBTITLE 1,PROPERTY_PRICE_DATA
CREATE TABLE IF NOT EXISTS `default`.`property_price_data` (
  `price` BIGINT,
  `town` STRING,
  `county` STRING,
  `beds` STRING,
  `baths` STRING,
  `propertyType` STRING,
  `address` STRING,
  `Website` STRING
) USING delta PARTITIONED BY (county) LOCATION 'dbfs:/user/hive/warehouse/property_price_data';

-- COMMAND ----------

-- DBTITLE 1,PROPERTY_PRICE_DATA_FINAL
CREATE TABLE IF NOT EXISTS `default`.`property_price_data_final` (
  `price` DOUBLE,
  `town` STRING,
  `county` STRING,
  `propertyType` STRING,
  `beds` STRING,
  `emi` DOUBLE
) USING delta
LOCATION 'dbfs:/user/hive/warehouse/property_price_data_final';

-- COMMAND ----------

-- DBTITLE 1,PROPERTY_PRICE_AVG
CREATE TABLE IF NOT EXISTS `default`.`property_price_avg` (
  `county` STRING,
  `town` STRING,
  `beds` STRING,
  `price` DOUBLE,
  `emi` DOUBLE
) USING delta
LOCATION 'dbfs:/user/hive/warehouse/property_price_avg';

-- COMMAND ----------

-- DBTITLE 1,TOWNS
CREATE TABLE IF NOT EXISTS `default`.`towns` (
  `town` STRING,
  `county` STRING,
  `latitude` STRING,
  `longitude` STRING
) USING delta
LOCATION 'dbfs:/user/hive/warehouse/towns';

-- COMMAND ----------

-- DBTITLE 1,PROPERTY_RENT_DATA
CREATE TABLE IF NOT EXISTS `default`.`property_rent_data` (
  `rent` BIGINT,
  `town` STRING,
  `county` STRING,
  `beds` STRING,
  `baths` STRING,
  `propertyType` STRING,
  `address` STRING,
  `Website` STRING
) USING delta PARTITIONED BY (county) LOCATION 'dbfs:/user/hive/warehouse/property_rent_data';

-- COMMAND ----------

-- DBTITLE 1,PROPERTY_RENT_DATA_FINAL
CREATE TABLE IF NOT EXISTS `default`.`property_rent_data_final` (
  `rent` DOUBLE,
  `town` STRING,
  `county` STRING,
  `propertyType` STRING,
  `beds` STRING
) USING delta
LOCATION 'dbfs:/user/hive/warehouse/property_rent_data_final';

-- COMMAND ----------

-- DBTITLE 1,PROPERTY_RENT_AVG
CREATE TABLE IF NOT EXISTS `default`.`property_rent_avg` (
  `county` STRING,
  `town` STRING,
  `beds` STRING,
  `rent` DOUBLE
) USING delta LOCATION 'dbfs:/user/hive/warehouse/property_rent_avg';

-- COMMAND ----------

-- DBTITLE 1,PROPERTY_PRICE_REGISTER
CREATE TABLE IF NOT EXISTS `default`.`property_price_register` (
  `SaleDate` DATE,
  `address` STRING,
  `town` STRING,
  `county` STRING,
  `price` DOUBLE,
  `propertyType` STRING,
  `saleMonth` STRING
) USING delta PARTITIONED BY (county) LOCATION 'dbfs:/user/hive/warehouse/property_price_register';

-- COMMAND ----------

-- DBTITLE 1,PROPERTY_RENT_REGISTER
CREATE TABLE IF NOT EXISTS `default`.`property_rent_register` (
  `NQuater` INT,
  `beds` STRING,
  `propertyType` STRING,
  `Location` STRING,
  `rent` DOUBLE,
  `town` STRING,
  `county` STRING,
  `quarterDate` DATE
) USING delta PARTITIONED BY (county) LOCATION 'dbfs:/user/hive/warehouse/property_rent_register';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS `default`.`property_price_register_final` (
  `SaleDate` DATE,
  `price` DOUBLE,
  `town` STRING,
  `county` STRING,
  `propertyType` STRING,
  `saleMonth` STRING
) USING delta LOCATION 'dbfs:/user/hive/warehouse/property_price_register_final';

-- COMMAND ----------

-- DBTITLE 1,PROPERTY_RENT_PRED
CREATE TABLE IF NOT EXISTS `default`.`property_price_pred` (
  `county` STRING,
  `saleMonth` STRING,
  `price` DOUBLE,
  nopandemic_price DOUBLE
) USING delta LOCATION 'dbfs:/user/hive/warehouse/property_price_pred';

-- COMMAND ----------

-- DBTITLE 1,PROPERTY_RENT_PRED
CREATE TABLE IF NOT EXISTS `default`.`property_rent_pred` (
  `county` STRING,
  `NQuater` STRING,
  `rent` DOUBLE,
  nopandemic_rent DOUBLE
) USING delta LOCATION 'dbfs:/user/hive/warehouse/property_rent_pred';