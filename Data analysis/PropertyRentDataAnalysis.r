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
    if((length(unique(in_layers$rent)) < 3 ) | (abs(e1071::skewness(in_layers$rent)) <= 0.5)) break;
    out_layer_stats <- boxplot.stats(in_layers$rent)$stats
    if (min(out_layer_stats) == max(out_layer_stats)) break;
    in_layers <- subset(in_layers,rent >= min(out_layer_stats) & rent <= max(out_layer_stats))
    i <- i + 1
  }
  return (in_layers)
}

aggregate_town <- function(df) {
  
}

# COMMAND ----------

# DBTITLE 1,Load table data to data frame
propertyDataDF <- as.data.frame(sql("select * from default.property_rent_data"))
Towns <- unique(subset(propertyDataDF,select=c('county','town','beds')))

# COMMAND ----------

# DBTITLE 1,Out layer removal demo
#df <- subset(propertyDataDF,county=='Donegal' & town=='Letterkenny' & beds == '3',c('rent'))
##print(nrow(df))
#print('Data')
#print(sort(df$rent))
#out_layer_stats <- boxplot.stats(df$rent)$stats
#print('Out layers')
#print(df$rent[df$rent < min(out_layer_stats) | df$rent > max(out_layer_stats)])
#boxplot(df$rent)

# COMMAND ----------

# DBTITLE 1,Remove out layers
print(paste('Number of rows before removing out layers : ' ,nrow(propertyDataDF)))
tmp <- propertyDataDF[FALSE,0]
for (i in 1:nrow(Towns))
  tmp <- rbind(tmp,remve_out_layers(subset(propertyDataDF,county == Towns[i,'county'] & town == Towns[i,'town'] & beds == Towns[i,'beds'])))

propertyDataDF <- tmp
print(paste('Number of rows before removing out layers : ' ,nrow(propertyDataDF)))


# COMMAND ----------

#library(VIM)
#missing_values <- aggr(as.data.frame(propertyDataDF), prop = FALSE, numbers = TRUE,cex.axis = .8)
#missing_values$missings$Percentage <- missing_values$missings$Count / nrow(propertyDataDF) * 100
#missing_values$missings

# COMMAND ----------

# DBTITLE 1,Skewness demo
#df <- subset(propertyDataDF,county=='Donegal' & town=='Letterkenny' & beds == '3')
#print(round(e1071::skewness(df$rent), 2))
#tmp <- remve_out_layers(df)
#df <- tmp
#print(round(e1071::skewness(df$rent), 2))
#tmp <- remve_out_layers(df)
#df <- tmp
#print(round(e1071::skewness(df$rent), 2))
#plot(density(df$rent), ylab = "Frequency", sub = "Skewness")


# COMMAND ----------

propertyAvgDataDF <- aggregate(x=propertyDataDF$rent,by=list(county = propertyDataDF$county, town = propertyDataDF$town, beds = propertyDataDF$beds),FUN='mean')
colnames(propertyAvgDataDF)[4] <- 'rent'
propertyDataSDF <- createDataFrame(propertyDataDF)
propertyAvgDataDF$rent <- round(propertyAvgDataDF$rent)
propertyAvgDataSDF <- createDataFrame(propertyAvgDataDF)

createOrReplaceTempView(propertyAvgDataSDF, "propertyAvgDataSDF")
sql('CREATE TABLE IF NOT EXISTS property_rent_avg as select * from propertyAvgDataSDF')

createOrReplaceTempView(propertyDataSDF, "propertyDataSDF")
sql('CREATE TABLE IF NOT EXISTS property_rent_data_final as select rent,town,county,propertyType,beds from propertyDataSDF')