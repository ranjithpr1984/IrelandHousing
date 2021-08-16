# Databricks notebook source
# DBTITLE 1,Import Library
library(e1071)
library(forecast)

library(lubridate)
library(SparkR)

# COMMAND ----------

# DBTITLE 1,Functions
remve_out_layers <- function(df) {
  in_layers <- df
  i <- 1
  while(i < 5 ) {
    if((length(unique(in_layers$price)) < 3 ) | (abs(e1071::skewness(in_layers$price)) <= 0.5)) break;
    out_layer_stats <- boxplot.stats(in_layers$price)$stats
    if (min(out_layer_stats) == max(out_layer_stats)) break;
    in_layers <- subset(in_layers,price >= min(out_layer_stats) & price <= max(out_layer_stats))
    i <- i + 1
  }
  return (in_layers)
}

fill_missing_months <- function(df) {
  startDate <- ymd(paste(min(df$saleMonth),'-01',sep = ''))
  endDate <- ymd(paste(max(df$saleMonth),'-01',sep = ''))
  allMonths <- format(seq(startDate, endDate, by = 'month'), "%Y-%m")
  j = 1
  for (i in 1:length(allMonths))
    if(allMonths[i] == df[j,'saleMonth']) j = j + 1
    else df <- rbind(df,c(df[j,'county'],allMonths[i],mean(c(df[j,'price'],df[j-1,'price']))))
 
  df$price <- as.numeric(df$price)
  return(df[order(df$saleMonth),])
}

predict_county_price <- function(df,pmonths) {
  #print(paste(df[1,'county'],df[nrow(df),'county']))
  maxYear <- as.numeric(substr(as.character(max(df$saleMonth)),1,4))
  maxMonth <- as.numeric(substr(as.character(max(df$saleMonth)),6,7))
  startYear <- as.numeric(substr(as.character(min(df$saleMonth)),1,4))
  startMonth <- as.numeric(substr(as.character(min(df$saleMonth)),6,7))
  dfNrow <- nrow(df)
  expNrow <- ((maxYear - startYear) * 12) + maxMonth - startMonth + 1
  df <- df[order(df$saleMonth),]
  if (dfNrow != expNrow) df <- fill_missing_months(df)
  price_ts <- ts(data = df$price, start = c(startYear, startMonth), frequency = 12)
  fit <- auto.arima(price_ts)
  ts_predict <- forecast(fit,pmonths)
  if (maxMonth < 10)
    startDate <- ymd(paste(as.character(maxYear),'-0',as.character(maxMonth),'-01',sep = '')) %m+% months(1)
  else
    startDate <- ymd(paste(as.character(maxYear),'-',as.character(maxMonth),'-01',sep = '')) %m+% months(1)
  endDate <- startDate %m+% months(pmonths - 1)
  predMonths <- seq(startDate, endDate, by = 'month')
  predDF <- df[FALSE,0]
  predDF[1:pmonths,'county'] <- df[1,'county']
  predDF$saleMonth <- format(predMonths, "%Y-%m")
  predDF$price <- as.numeric(ts_predict[['mean']])
  return (rbind(df,predDF))
}

# COMMAND ----------

# DBTITLE 1,Load table data to data frame
propertyDataDF <- as.data.frame(sql("select * from property_price_register"))

# COMMAND ----------

# DBTITLE 1,Convert Irish property type to English
propertyDataDF$propertyType[grepl("Teach.*Nua", propertyDataDF$propertyType)] <- "New Dwelling house /Apartment"
propertyDataDF$propertyType[grepl("Teach.*imhe", propertyDataDF$propertyType)] <- "Second-Hand Dwelling house /Apartment"

# COMMAND ----------

# DBTITLE 1,Remove out layers
Towns <- unique(subset(propertyDataDF,select=c('county','town','saleMonth')))
print(paste('Number of rows before removing out layers : ' ,nrow(propertyDataDF)))
tmp <- propertyDataDF[FALSE,0]
for (i in 1:nrow(Towns))
  tmp <- rbind(tmp,remve_out_layers(subset(propertyDataDF,county == Towns[i,'county'] & town == Towns[i,'town'] & saleMonth == Towns[i,'saleMonth'])))

propertyDataDF <- tmp
print(paste('Number of rows before removing out layers : ' ,nrow(propertyDataDF)))


# COMMAND ----------

# DBTITLE 1,Aggregate per town and month
propertyAvgTownDF <- aggregate(x=propertyDataDF$price,by=list(county = propertyDataDF$county, town = propertyDataDF$town, saleMonth = propertyDataDF$saleMonth),FUN='mean')
colnames(propertyAvgTownDF)[4] <- 'price'
display(propertyAvgTownDF)

# COMMAND ----------

# DBTITLE 1,Add current month data
propertyAvgTownThisMonthDF <- as.data.frame(sql("select county,town,year(current_timestamp())||'-'||lpad(month(current_timestamp()),2,'0') saleMonth,avg(price) price from property_price_avg group by county,town"))
thisMonth <- format(Sys.Date(), "%Y-%m")
propertyAvgTownThisMonthDF2 <- subset(propertyAvgTownDF, saleMonth == thisMonth)

if(nrow(propertyAvgTownThisMonthDF2) > 0 ) {
  tmp <- rbind(propertyAvgTownThisMonthDF,propertyAvgTownThisMonthDF2)
  propertyAvgTownThisMonthDF <- aggregate(x=tmp$price,by=list(county = tmp$county, town = tmp$town, saleMonth = tmp$saleMonth),FUN='mean')
  colnames(propertyAvgTownThisMonthDF)[4] <- 'price'
}
  
propertyAvgTownDF <- rbind(propertyAvgTownDF,propertyAvgTownThisMonthDF)


# COMMAND ----------

# DBTITLE 1,Aggregate by county and month
propertyAvgCountyDF <- aggregate(x=propertyAvgTownDF$price,by=list(county = propertyAvgTownDF$county, saleMonth = propertyAvgTownDF$saleMonth),FUN='mean')
colnames(propertyAvgCountyDF)[3] <- 'price'
display(propertyAvgCountyDF)

# COMMAND ----------

# DBTITLE 1,Predict future house price - County
counties <- unique(propertyAvgCountyDF['county'])[,1]
propertyAvgCountyPredDF1 <- propertyAvgCountyDF[FALSE,0]

for (i in 1:length(counties)) 
  propertyAvgCountyPredDF1 <- rbind(propertyAvgCountyPredDF1,predict_county_price(subset(propertyAvgCountyDF,county == counties[i]),16))

# COMMAND ----------

# DBTITLE 1,Predict future house price data prior to COVID-19 - County
counties <- unique(propertyAvgCountyDF['county'])[,1]
propertyAvgCountyPredDF2 <- propertyAvgCountyDF[FALSE,0]

for (i in 1:length(counties)) 
  propertyAvgCountyPredDF2 <- rbind(propertyAvgCountyPredDF2,predict_county_price(subset(propertyAvgCountyDF,county == counties[i] & saleMonth < '2020-01'),36))

# COMMAND ----------

# DBTITLE 1,Save data to databricks table
propertyAvgCountyPredDF1$price <- round(propertyAvgCountyPredDF1$price)
propertyAvgCountyPredDF2$price <- round(propertyAvgCountyPredDF2$price)

createOrReplaceTempView(createDataFrame(propertyAvgCountyPredDF1), "propertyAvgCountyPredSDF1")
createOrReplaceTempView(createDataFrame(propertyAvgCountyPredDF2), "propertyAvgCountyPredSDF2")
sql('CREATE TABLE IF NOT EXISTS property_price_pred as select x.county,x.saleMonth,x.price,y.price nopandemic_price from propertyAvgCountyPredSDF1 x join propertyAvgCountyPredSDF2 y on x.county=y.county and x.saleMonth=y.saleMonth')

createOrReplaceTempView(createDataFrame(propertyDataDF), "propertyDataSDF")
sql('CREATE TABLE IF NOT EXISTS property_price_register_final as select SaleDate,price,town,county,propertyType,saleMonth from propertyDataSDF')