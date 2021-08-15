# Databricks notebook source
# DBTITLE 1,Import Library
library(SparkR)
library(e1071)

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

# COMMAND ----------

# DBTITLE 1,Load table data to data frame
propertyDataDF <- as.data.frame(sql("select * from default.property_price_data"))
Towns <- unique(subset(propertyDataDF,select=c('county','town','beds')))

# COMMAND ----------

# DBTITLE 1,Skewness demo
df <- subset(propertyDataDF,county=='Donegal' & town=='Stranorlar' & beds == '4')
plot(density(df$price), main ='Density Plot plot for Stranorlar, Co. Donegal', ylab = "Frequency", sub = paste("Skewness : ", round(e1071::skewness(df$price),2)))

# COMMAND ----------

# DBTITLE 1,Out layer removal demo
df <- subset(propertyDataDF,county=='Donegal' & town=='Stranorlar' & beds == '4',c('price'))
print(nrow(df))
print('Data')
print(sort(df$price))
out_layer_stats <- boxplot.stats(df$price)$stats
print('boxplot.stats')
print(out_layer_stats)
print('Out layers')
print(df$price[df$price < min(out_layer_stats) | df$price > max(out_layer_stats)])
boxplot(df$price)

# COMMAND ----------

# DBTITLE 1,Remove out layers
print(paste('Number of rows before removing out layers : ' ,nrow(propertyDataDF)))
tmp <- propertyDataDF[FALSE,0]
for (i in 1:nrow(Towns))
  tmp <- rbind(tmp,remve_out_layers(subset(propertyDataDF,county == Towns[i,'county'] & town == Towns[i,'town'] & beds == Towns[i,'beds'])))

propertyDataDF <- tmp
print(paste('Number of rows before removing out layers : ' ,nrow(propertyDataDF)))


# COMMAND ----------

interestRate = 2.745
months = 25*12
rate = interestRate/(12*100)

propertyDataDF$emi <- round((propertyDataDF$price * 0.9) * rate * ((1+rate)**months)/((1+rate)**months - 1))

# COMMAND ----------

#library(VIM)
#missing_values <- aggr(as.data.frame(propertyDataDF), prop = FALSE, numbers = TRUE,cex.axis = .8)
#missing_values$missings$Percentage <- missing_values$missings$Count / nrow(propertyDataDF) * 100
#missing_values$missings

# COMMAND ----------

propertyAvgDataDF <- aggregate(cbind(price, emi) ~ county + town + beds, data = propertyDataDF, mean)
propertyDataSDF <- createDataFrame(propertyDataDF)
propertyAvgDataDF$price <- round(propertyAvgDataDF$price)
propertyAvgDataDF$emi <- round(propertyAvgDataDF$emi)
propertyAvgDataSDF <- createDataFrame(propertyAvgDataDF)

createOrReplaceTempView(propertyAvgDataSDF, "propertyAvgDataSDF")
sql('CREATE TABLE IF NOT EXISTS property_price_avg as select * from propertyAvgDataSDF')

createOrReplaceTempView(propertyDataSDF, "propertyDataSDF")
sql('CREATE TABLE IF NOT EXISTS property_price_data_final as select price,town,county,propertyType,beds,emi from propertyDataSDF')