setwd("/Users/mac/Downloads/egyetem/TDK/magyar_petered_main")

# ==== 1. Read-in shapefile of settlements ====
setwd("/Users/mac/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data")

library(sf)
library(leaflet)
library(stringi)
library(dplyr)

shape_df <- st_read('kozighatarok/admin8.shp')
# data from: https://data2.openstreetmap.hu/hatarok/index.php?admin=8
shape_df <- st_make_valid(shape_df)
shape_df <- st_transform(shape_df, crs = 4326)

duplicates <- data.frame(table(shape_df$NAME)[table(shape_df$NAME)>1])
  # some settlements are duplicated
shape_df[shape_df$NAME %in% duplicates$Var1,]
shape_df <- shape_df %>% distinct(NAME, .keep_all=TRUE) # drop duplicates

# st_write(shape_df, 'admin8v2.shp') # new dataframe

# ==== 2. Read-in settlements affected by the campaign ====



