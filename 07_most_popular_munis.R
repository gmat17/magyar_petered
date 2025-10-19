setwd("~/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data")
# --- Get frequency table ----
library(plyr)
munis <- readxl::read_excel('ksh_data_concated.xlsx')
munis_df <- data.frame()
munis_df <- munis[1:10,'id']
for (i in colnames(munis)[7:51]) {
  top10 <- munis[order(munis[[i]], decreasing = TRUE), ][1:10, "name"]
  munis_df[[i]] <- top10
}
munis_df <- munis_df[,colnames(munis_df)!='id']
values <- as.data.frame(table(unlist(munis_df)))

values <- values[order(values$Freq, decreasing=TRUE),]
once_top10 <- length(values[values$Freq==1,'Var1'])
twice_top10 <- length(values[values$Freq==2,'Var1'])
three_top10 <- length(values[values$Freq==3,'Var1'])
four_top10 <- length(values[values$Freq==4,'Var1'])
top_values <- values[values$Freq>4,]

add_values <- data.frame(Var1=c('4-times top10','3-times top10','2-times top10','1-times top10'),
           Freq=c(four_top10,three_top10,twice_top10,once_top10))

result <- rbind(top_values,add_values)

# ---- Add geometry to the values table ----
library(sf)
library(leaflet)
library(ggplot2)

# load shapefile of munis
shape_df <- st_read('kozighatarok/admin8.shp')
shape_df <- st_make_valid(shape_df)
shape_df <- st_transform(shape_df, crs = 4326)

# load hungary's shape
hun_shape <- st_read('kozighatarok/admin2.shp')
hun_shape <- st_make_valid(hun_shape)
hun_shape <- st_transform(hun_shape, crs = 4326)

# load munis' centre point
munis <- merge(values, shape_df, by.x='Var1',by.y='NAME')
munis <- st_as_sf(munis)

points <- read.csv('municipality_lonlat.csv')[,c('name', 'x','y')]
munis <- merge(munis, points, by.x='Var1', by.y='name')
munis[munis$Freq<5,c('x','y')] <- c(NA,NA)

# ---- Plot the result ----
ggplot(munis) +
  geom_sf(data=hun_shape, fill='white', size=0.3,color='black') +
  geom_sf(aes(fill = Freq), color = 'white', size=0.01) +
  geom_point(aes(x = x, y = y), color = 'red', size = 2) +
  geom_label(aes(x = x, y = y, label = Var1),
            hjust = 0, nudge_y = 0.1,
            size = 3, fill='white',color = "black") +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal() +
  theme(legend.position='bottom') +
  labs(title = 'A kiválasztott adatokban legmagasabb \n értékekkel rendelkező települések', ) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
        axis.title = element_blank(), axis.text=element_blank(), panel.grid=element_blank())
