# Databricks notebook source
# DBTITLE 1,Import Library
library(e1071)
library(forecast)

library(lubridate)
library(SparkR)

# COMMAND ----------

# DBTITLE 1,Functions
predict_county_rent <- function(df,pquarter) {
  #df = subset(propertyAvgCountyDF,county == 'Carlow')
  #pquarter = 5
  startYear <- as.numeric(substr(as.character(min(df$NQuater)),1,4))
  startQuarter <- as.numeric(substr(as.character(min(df$NQuater)),5,5))
  maxYear <- as.numeric(substr(as.character(max(df$NQuater)),1,4))
  maxQuarter <- as.numeric(substr(as.character(max(df$NQuater)),5,5))
  maxMonth <- (maxQuarter * 3)
  rent_ts <- ts(data = df$rent, start = c(startYear, startQuarter), frequency = 4)
  df <- df[order(df$NQuater),]
  fit <- auto.arima(rent_ts)
  ts_predict <- forecast(fit,pquarter)
  if (maxMonth < 10)
    startDate <- ymd(paste(as.character(maxYear),'-0',as.character(maxMonth),'-01',sep = '')) %m+% months(1)
  else
    startDate <- ymd(paste(as.character(maxYear),'-',as.character(maxMonth),'-01',sep = '')) %m+% months(1)
  endDate <- startDate %m+% months((pquarter-1)*3)
  predMonths <- seq(startDate, endDate, by = 'quarter')
  predDF <- df[FALSE,0]
  predDF[1:pquarter,'county'] <- df[1,'county']
  predDF$NQuater <- as.numeric(paste(format(predMonths, "%Y"),sub("Q","",quarters(predMonths)),sep=''))
  predDF$rent <- as.numeric(ts_predict[['mean']])
  return (rbind(df,predDF))
}

# COMMAND ----------

# DBTITLE 1,Load table data to data frame
propertyDataDF <- as.data.frame(sql("select NQuater,rent, town,county from property_rent_register"))

# COMMAND ----------

# DBTITLE 1,Aggregate per town and month
propertyAvgTownDF <- aggregate(x=propertyDataDF$rent,by=list(county = propertyDataDF$county, town = propertyDataDF$town, NQuater = propertyDataDF$NQuater),FUN='mean')
colnames(propertyAvgTownDF)[4] <- 'rent'
display(propertyAvgTownDF)

# COMMAND ----------

# DBTITLE 1,Add current month data
propertyAvgTownThisMonthDF <- as.data.frame(sql("select county,town,year(current_timestamp())||quarter(current_timestamp()) NQuater,round(avg(rent)) rent from property_rent_avg group by county,town"))
thisQuarter <- paste(format(Sys.Date(), "%Y"),sub("Q","",quarters(Sys.Date())),sep='')
propertyAvgTownThisMonthDF2 <- subset(propertyAvgTownDF, NQuater == thisQuarter)

if(nrow(propertyAvgTownThisMonthDF2) > 0 ) {
  tmp <- rbind(tmp,propertyAvgTownThisMonthDF,propertyAvgTownThisMonthDF2)
  propertyAvgTownThisMonthDF <- aggregate(x=tmp$rent,by=list(county = tmp$county, town = tmp$town, NQuater = tmp$NQuater),FUN='mean')
  colnames(propertyAvgTownThisMonthDF)[4] <- 'rent'
}
  
propertyAvgTownDF <- rbind(propertyAvgTownDF,propertyAvgTownThisMonthDF)

display(propertyAvgTownDF)

# COMMAND ----------

# DBTITLE 1,Aggregate by county and quarter
propertyAvgCountyDF <- aggregate(x=propertyAvgTownDF$rent,by=list(county = propertyAvgTownDF$county, NQuater = propertyAvgTownDF$NQuater),FUN='mean')
colnames(propertyAvgCountyDF)[3] <- 'rent'
display(propertyAvgCountyDF)

# COMMAND ----------

# DBTITLE 1,Predict future rent - County
counties <- unique(propertyAvgCountyDF['county'])[,1]
propertyAvgCountyPredDF1 <- propertyAvgCountyDF[FALSE,0]

for (i in 1:length(counties)) 
  propertyAvgCountyPredDF1 <- rbind(propertyAvgCountyPredDF1,predict_county_rent(subset(propertyAvgCountyDF,county == counties[i]),5))

# COMMAND ----------

# DBTITLE 1,Predict future house price data prior to COVID-19 - County
counties <- unique(propertyAvgCountyDF['county'])[,1]
propertyAvgCountyPredDF2 <- propertyAvgCountyDF[FALSE,0]

for (i in 1:length(counties)) 
  propertyAvgCountyPredDF2 <- rbind(propertyAvgCountyPredDF2,predict_county_rent(subset(propertyAvgCountyDF,county == counties[i] & NQuater < '20201'),12))

# COMMAND ----------

# DBTITLE 1,Save data to databricks table
propertyAvgCountyPredDF1$rent <- round(propertyAvgCountyPredDF1$rent)
propertyAvgCountyPredDF2$rent <- round(propertyAvgCountyPredDF2$rent)

createOrReplaceTempView(createDataFrame(propertyAvgCountyPredDF1), "propertyAvgCountyPredSDF1")
createOrReplaceTempView(createDataFrame(propertyAvgCountyPredDF2), "propertyAvgCountyPredSDF2")
sql('CREATE TABLE IF NOT EXISTS property_rent_pred as select x.county,x.NQuater,x.rent,y.rent nopandemic_rent from propertyAvgCountyPredSDF1 x join propertyAvgCountyPredSDF2 y on x.county=y.county and x.NQuater=y.NQuater')