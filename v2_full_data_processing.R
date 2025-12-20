setwd("/Users/mac/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data")

# ==== 1. Read-in shapefile of settlements ====
library(sf)
library(leaflet)
library(stringi)
library(dplyr)

shape_df <- st_read('kozighatarok/admin8.shp')
colnames(shape_df) <- tolower(colnames(shape_df))
# data from: https://data2.openstreetmap.hu/hatarok/index.php?admin=8
shape_df <- st_make_valid(shape_df)
shape_df <- st_transform(shape_df, crs = 4326)

duplicates <- data.frame(table(shape_df$name)[table(shape_df$name)>1])
  # some settlements are separated from each other
shape_df <- shape_df %>% group_by(name) %>% summarise(geometry=st_union(geometry),.groups='drop')
  # grouped by

# st_write(shape_df, 'admin8v2.shp') # new shapefile

# load hungary shapefile for the plots
hun_shape <- st_read('kozighatarok/admin2.shp')
hun_shape <- st_make_valid(hun_shape)
hun_shape <- st_transform(hun_shape, crs = 4326)

# ==== 2. Read-in settlements affected by the campaign ====
library(ggplot2)

campaign <- readxl::read_excel('magyar_petered_telepulesek.xlsx')
campaign

# load campaign to shape_df as logical data
shape_df$is_mped <- shape_df$name %in% campaign$is_magyar_petered
shape_df$is_fideszed <- shape_df$name %in% campaign$is_fideszed
shape_df$is_dked <- shape_df$name %in% campaign$is_dked

# plot the result
ggplot(shape_df) +
  geom_sf(data=hun_shape, fill='white', size=0.3,color='black') +
  geom_sf(aes(fill = is_mped)) +
  scale_fill_manual(
    values = c('FALSE'='#ed4551', 'TRUE'='#24b574'),
    labels = c('Nem', 'Igen'),
    name = 'Meglátogatott?'
  ) +  theme_minimal() +
  theme(legend.position='bottom') +
  labs(title = 'Magyar Péter által országjárt települések') +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
        axis.title = element_blank(), axis.text=element_blank(), panel.grid=element_blank()) +
  guides(fill = guide_legend(title.position = "top", title.hjust = 0.5))

ggplot(shape_df) +
  geom_sf(data=hun_shape, fill='white', size=0.3,color='black') +
  geom_sf(aes(fill = is_fideszed)) +
  scale_fill_manual(
    values = c('TRUE'='#fd8100', 'FALSE'='#FFFFFF'),
    labels = c('Nem', 'Igen'),
    name = 'Meglátogatott?'
  ) +
  theme_minimal() +
  theme(legend.position='bottom') +
  labs(title = 'Orbán Viktor vagy Deutsch Tamás által országjárt települések') +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
        axis.title = element_blank(), axis.text=element_blank(), panel.grid=element_blank()) +
  guides(fill = guide_legend(title.position = "top", title.hjust = 0.5))

ggplot(shape_df) +
  geom_sf(data=hun_shape, fill='white', size=0.3,color='black') +
  geom_sf(aes(fill = is_dked)) +
  scale_fill_manual(
    values = c('TRUE'='#007FFF', 'FALSE'='#f1cad3'),
    labels = c('Nem', 'Igen'),
    name = 'Meglátogatott?'
  ) +
  theme_minimal() +
  theme(legend.position='bottom') +
  labs(title = 'Gyurcsány Ferenc vagy Dobrev Klára által országjárt települések') +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
        axis.title = element_blank(), axis.text=element_blank(), panel.grid=element_blank()) +
  guides(fill = guide_legend(title.position = "top", title.hjust = 0.5))

# ==== 3. Add points to the dataframe ====
points <- st_read('kozighatarok/place.shp')
points <- points[,c('NAME','geometry')]
points <- st_make_valid(points)
points <- st_transform(points, crs=4326)
colnames(points) <- c('name', 'geometry')

# convert to dataframe to add to the table
# just to check the settlements, otherwise the points will be stored in another dataframe
points.df <- as.data.frame(points)
colnames(points.df) <- c('name', 'point')

df <- merge(shape_df, points.df, by.x='name', by.y='name')
# filter for those points which are in the df
points <- points[points$name %in% df$name,]

# save the result to a shapefile
# st_write(points, 'placev2.shp')

# ==== 4. Plot points ====
ggplot(shape_df) +
  geom_sf(data=hun_shape, fill='white', size=0.3,color='black') +
  geom_sf(data=points, fill='white', size=0.3,color='black') +
  geom_sf(aes(fill = is_mped)) +
  scale_fill_manual(
    values = c('FALSE'='#ed4551', 'TRUE'='#24b574'),
    labels = c('Nem', 'Igen'),
    name = 'Meglátogatott?'
  ) + theme_minimal() +
  theme(legend.position='bottom') +
  labs(title = 'Magyar Péter által országjárt települések') +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
        axis.title = element_blank(), axis.text=element_blank(), panel.grid=element_blank()) +
  guides(fill = guide_legend(title.position = "top", title.hjust = 0.5))

ggplot() +
  geom_sf(data=hun_shape, fill='white', size=0.3,color='black') +
  geom_sf(data=points, fill='white', size=0.3,color='black') +
  geom_sf(data=shape_df, fill='transparent', size=0.1,color='black') +
  theme_minimal() +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
        axis.title = element_blank(), axis.text=element_blank(), panel.grid=element_blank()) +
  guides(fill = guide_legend(title.position = "top", title.hjust = 0.5))

# ==== 5. Read-in KSH data ====
# ==== 0. Disclaimer ====
# Ahogy látható, az ebben a kódban használt táblák egy másik mappában vannak.
# Ennek oka, hogy a TEIR, a Timea és a Népszámlálás oldaláról letöltött adatokkal
# nem szeretném feleslegesen telíteni a GitHub tárhelyemet, az adatok bárki számára elérhetőek az
# egyetlen hozzáadott értékem, hogy kivártam, amíg a Timea betölti a lekérdezést - mondjuk 
# dicsvágyam nem hagyja, hogy ezt oly' kis munkának állítsam be.
# Az ezen kód által kiadott Excel fájlt természetesen feltöltöm a GitHubra, hiszen az már
# elengedhetetlen az átláthatósághoz. S akkor jöjjön a kód (és előtte az angol verzió):

# As you can see, the tables used in this code are in a different folder. The reason is that
# I don't want to waste my GitHub disk for the TEIR, Timea and Nepszamlalas raw data which are
# downloadable for everybody, the only value added from my side was waiting that Timea loads the
# query - however my ambition doesn't let me to appear it as a little work.
# The result Excel file will be on the GitHub of course, because it is necessary for the trans-
# parency. And after this loooong writing. here's the code:

setwd("~/Downloads/egyetem/TDK/magyar_petered_main/ksh_data")
# ==== 1. Loading tables ====

# --- TEIR table ----
goods <- list.files('1_jó')
teir <- readxl::read_excel('TEIR_TÁBLÁZAT 2025922_14-23-0.xlsx', sheet='lap_0')
teir <- teir[!is.na(teir$kod),]
check_teir <- function(x){if (grepl(" \\*$", x)){substr(x, 1, nchar(x) - 2)}else{x}}
teir$...1 <- sapply(teir$...1,FUN = check_teir)

# --- Points ----
setwd("~/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data")
file <- '~/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data/municipality_lonlat.csv'
base_table <- read.csv(file)
length(base_table$name)
base_table <- base_table[!duplicated(base_table$name),]

# ---- Load 28 files from 1_jo (Timea data) ----
rename_col <- function(df, old_name, new_name){
  names(df)[names(df) == old_name] <- new_name
  return (df)
}

# add code to base table
code_df <- read.csv(paste0('1_jó/', goods[1]), sep=';')[,c('ELEM_KOD','TELEP_NEV')]
base_table <- merge(code_df, base_table, by.x='TELEP_NEV','name')
names(base_table) <- c('name', 'id', "place", "is_mped", "x", "y")
convert_hun_number <- function(x){gsub(',','.',x)}

for (i in 1:length(goods)){
  mini_df <- read.csv(paste0('1_jó/', goods[i]), sep=';')[,c('ELEM_KOD','VALUE')]
  mini_df$VALUE <- as.numeric(sapply(mini_df$VALUE,convert_hun_number))
  mini_df <- rename_col(mini_df, 'VALUE', goods[i])
  base_table <- merge(base_table, mini_df, by.x='id', by.y = 'ELEM_KOD', all.x = TRUE)
}

# ---- Join 28 files with TEIR ----
length(teir$...1)
length(base_table$name)

# base_table <- merge(base_table, teir, by.x='id', by.y='kod', all.x=TRUE)

# ---- Load 3 files from 0_problemas (Timea data) ----
# In these tables, the interesting thing is the change from 2012 to 2023.

load_table <- function(filename){
  data <- read.csv(paste0('0_problémás/', filename), sep=';')
  data <- data[,c('ELEM_KOD','VALUE')]
  data$VALUE <- as.numeric(sapply(data$VALUE,convert_hun_number))
  return(data)
}

get_diff <- function(filename_basis, name_diff){
  data2023 <- load_table(paste0(filename_basis,'_2023.csv'))
  data2012 <- load_table(paste0(filename_basis,'_2012.csv'))
  data <- merge(data2023, data2012, 'ELEM_KOD', 'ELEM_KOD')
  data$diff <- data$VALUE.x-data$VALUE.y
  data <- rename_col(data, 'diff', name_diff)
  return(data[,c('ELEM_KOD', name_diff)])
}
# Száz km2 területre jutó közút
routes <- get_diff('Száz km2 területre jutó közút', 'len_routes_diff')

# Épített lakás tízezer lakosra
flats <- get_diff('Épített lakás tízezer lakosra', 'flats')

# Ezer lakóra jutó lakásépítési engedélyek és bejelentések
buildings <- get_diff('Ezer lakóra jutó lakásépítési engedélyek és bejelentések', 'building_permissions')

# Add them to base_table
base_table <- merge(base_table, routes, by.x='id', by.y='ELEM_KOD')
base_table <- merge(base_table, flats, by.x='id', by.y='ELEM_KOD')
base_table <- merge(base_table, buildings, by.x='id', by.y='ELEM_KOD')

# Load detailed age and education data from Nepszamlalas 2022
load_nepszamlalas <- function(filename,getpop=FALSE){
  df <- readxl::read_excel(filename)
  df[is.na(df)] <- 0
  df$pop <- rowSums(df[,2:ncol(df)])
  for(i in colnames(df)[2:(ncol(df)-1)]){
    df[,i] <- df[,i]/df$pop
  }
  if(getpop==FALSE){df <- df[,1:ncol(df)-1]}
  return(df)
}

# Age
age <- load_nepszamlalas('ksh-census2022-korcsopok.xlsx')
base_table <- merge(base_table, age, by.x='name', by.y='...1')

# Education
edu <- load_nepszamlalas('ksh-census2022-iskola.xlsx',getpop=TRUE)
base_table <- merge(base_table, edu, by.x='name', by.y='...1')

# ---- Exporting the final table ----
writexl::write_xlsx(base_table, 'ksh_data_concated.xlsx')