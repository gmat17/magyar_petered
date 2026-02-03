setwd("/Users/mac/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data")

# ==== 1. Geodata preparation ==== 
# ==== 1.1. Read-in shapefile of settlements ====
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
shape_df <- shape_df %>% 
  select(name, geometry) %>%
  group_by(name) %>%
  dplyr::summarise(geometry = st_union(geometry), .groups = "drop")
  # grouped by
# st_write(shape_df, 'admin8v2.shp') # new shapefile

# load hungary shapefile for the plots
hun_shape <- st_read('kozighatarok/admin2.shp')
hun_shape <- st_make_valid(hun_shape)
hun_shape <- st_transform(hun_shape, crs = 4326)

# ==== 1.2. Read-in settlements affected by the campaign ====
library(ggplot2)

campaign <- readxl::read_excel('magyar_petered_telepulesek.xlsx')
plot.path <- '/Users/mac/Downloads/egyetem/TDK/magyar_petered_main/plots/'

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
# ggsave(filename=paste0(plot.path, '01_mp_orszagjaras.png'), 
#   width = 2000, height = 1500, units = "px", dpi = 300)

# ==== 1.3. Plot the result ====
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
# ggsave(filename=paste0(plot.path, '02_fidesz_orszagjaras.png'), 
#   width = 2000, height = 1500, units = "px", dpi = 300)

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
# ggsave(filename=paste0(plot.path, '03_dk_orszagjaras.png'), 
# width = 2000, height = 1500, units = "px", dpi = 300)

# ==== 1.4. Add points to the dataframe ====
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
df$x <- st_coordinates(points)[,1]
df$y <- st_coordinates(points)[,2]

# save the result to a shapefile
# st_write(points, 'placev2.shp')

ggplot() +
  geom_sf(data=hun_shape, fill='white', size=0.3,color='black') +
  geom_sf(data=points, fill='white', size=0.3,color='black') +
  geom_sf(data=shape_df, fill='transparent', size=0.1,color='black') +
  theme_minimal() +
  theme(legend.position='bottom') +
  labs(title = 'Magyarországi települések középpontja és a települések határai') +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
        axis.title = element_blank(), axis.text=element_blank(), panel.grid=element_blank()) +
  guides(fill = guide_legend(title.position = "top", title.hjust = 0.5))
# ggsave(filename=paste0(plot.path, '04_telepulesek.png'), 
# width = 2000, height = 1500, units = "px", dpi = 300)


# ==== 2. KSH data preparation ====
#  disclaimer
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
goods <- list.files('1_jó')

# ---- 2.1. Load 28 files from 1_jo (Timea data) ----
rename_col <- function(df, old_name, new_name){
  names(df)[names(df) == old_name] <- new_name
  return (df)
}

# add code to base table
goods <- list.files('1_jó')
code_df <- read.csv(paste0('1_jó/', goods[1]), sep=';')[,c('ELEM_KOD','TELEP_NEV')]
colnames(code_df) <- c('id','name')
df <- merge(code_df, df, by.x='name',by.y='name')
convert_hun_number <- function(x){gsub(',','.',x)}

for (i in 1:length(goods)){
  mini_df <- read.csv(paste0('1_jó/', goods[i]), sep=';')[,c('ELEM_KOD','VALUE')]
  mini_df$VALUE <- as.numeric(sapply(mini_df$VALUE,convert_hun_number))
  mini_df <- rename_col(mini_df, 'VALUE', goods[i])
  df <- merge(df, mini_df, by.x='id', by.y = 'ELEM_KOD', all.x = TRUE)
}

# ---- 2.2. Load 3 files from 0_problemas (Timea data) ----
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
df <- merge(df, routes, by.x='id', by.y='ELEM_KOD')
df <- merge(df, flats, by.x='id', by.y='ELEM_KOD')
df <- merge(df, buildings, by.x='id', by.y='ELEM_KOD')

# --- 2.3. Load detailed age and education data from Nepszamlalas 2022 ----
# A Népszámlálásnál már többet módosítottam a táblán, levágtam a felesleges sorokat, és a …
# karaktert 0-ra cseréltem. A részletes változtatások a TDK-ban vannak leírva. 

# I modified the tables a little bit stronger at the Nepszamlalas, I've cut the unnecesarry rows
# and the … character have been replaced by 0. The detailed changes are written in the TDK paper.

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
df <- merge(df, age, by.x='name', by.y='...1')

# Education
edu <- load_nepszamlalas('ksh-census2022-iskola.xlsx',getpop=TRUE)
df <- merge(df, edu, by.x='name', by.y='...1')

# Religion
rel <- load_nepszamlalas('ksh-census2022-vallas.xlsx')
colnames(rel) <- c(colnames(rel)[1:10],'ateist', 'rel_no_ans')
rel$christian <- rowSums(rel[,c('Katolikus', 'Református', 'Evangélikus', 'Ortodox keresztény',
'Más keresztény felekezet')])
df <- merge(df, rel[,c('...1','christian', 'ateist', 'rel_no_ans')], by.x='name', by.y='...1')

roma <- load_nepszamlalas('ksh-census2022-nemzetiseg.xlsx')
colnames(roma) <- c('city','hun', 'roma', 'other_nat', 'roma_no_ans')
# rowSums(roma[,c('hun', 'roma', 'other_nat', 'no_ans')])
df <- merge(df, roma, by.x='name',by.y='city')

# Set up column names
setwd("/Users/mac/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data")
desc <- readxl::read_excel('ksh_data_concated.xlsx', sheet = 'description')
desc$description <- stri_trans_nfc(desc$description)
csvin <- desc[grepl('.csv',desc$description),]
csvin$description <- substr(csvin$description, 1, nchar(csvin$description) - 4)
desc[csvin$name==desc$name,]$description <- csvin$description

colnames(df)[10:54] <- desc$name
df[is.na(df)] <- 0

# ---- 2.5. Plot data ----
# can be found in plot_ksh_data.R

# ==== 3. Descriptive statistic of timea ====
# --- 3.1. Get frequency table ----
library(plyr)
munis_df <- data.frame()
munis_df <- df[1:10,'id']

munis_df <- data.frame(id = df[1:10,'id'])

for (i in colnames(df)[8:52]) {
  top10 <- head(df[order(df[[i]], decreasing = TRUE), "name"], 10)
  munis_df[[i]] <- top10
}

values <- as.data.frame(table(unlist(munis_df)))

values <- values[order(values$Freq, decreasing=TRUE),]
once_top10 <- length(values[values$Freq==1,'Var1'])
twice_top10 <- length(values[values$Freq==2,'Var1'])
three_top10 <- length(values[values$Freq==3,'Var1'])
four_top10 <- length(values[values$Freq==4,'Var1'])
top_values <- values[values$Freq>4,]

add_values <- data.frame(Var1=c('4-times top10','3-times top10','2-times top10','1-times top10'),
           Freq=c(four_top10,three_top10,twice_top10,once_top10))

top.result <- rbind(top_values,add_values)

# ---- 3.2. Add geometry to the values table ----
munis <- merge(values, df, by.x='Var1',by.y='name')
munis <- st_as_sf(munis)
munis[munis$Freq<5,c('x','y')] <- c(NA,NA)

ggplot(munis) +
  geom_sf(data=hun_shape, fill='white', size=0.3,color='black') +
  geom_sf(aes(fill = Freq), color = 'white', size=0.01) +
  geom_point(aes(x = x, y = y), color = 'red', size = 2) +
  geom_label(aes(x = x, y = y, label = Var1),
            hjust = 0, nudge_y = 0.1,
            size = 1.5, fill='white',color = "black") +
  scale_fill_viridis_b(option = 'viridis', name='Gyakoriság') +
  theme_minimal() +
  theme(legend.position='bottom') +
  labs(title = 'A kiválasztott adatokban legmagasabb \n értékekkel rendelkező települések') +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
        axis.title = element_blank(), axis.text=element_blank(), panel.grid=element_blank())
# ggsave(filename=paste0(plot.path, '05_top_telepulesek.png'), 
#   width = 2000, height = 1500, units = "px", dpi = 300)

# ==== 4. PCA and correlations ====
# apply logs
test <- df
for (i in desc$name){
  if (desc[desc$name==i,'using_log1p']==1) {df[[i]] <- log1p(df[[i]])}
}

# ---- 4.1. Correlation matrix ----
library(corrplot)

cor.mat <- cor(as.data.frame(df[,10:62]))
# png(filename = paste0(plot.path, '06_corr_matrix.png'), width = 1200, height = 1200, res = 120)
corrplot(cor(as.data.frame(df[,10:62])), method='square', type='upper', diag=FALSE, tl.cex = 0.6)
# dev.off()

# only medium and strong correlation
cor.mat[abs(cor.mat)<0.3] <- 0
corrplot(cor.mat, method='square', type='upper', diag=FALSE, tl.cex = 0.6)
(sum(abs(cor.mat)>0)-nrow(cor.mat))/2 # 159
# easy to interpret the similar values
  # upper-left corner: 
    # flat_sewage, szja, flat_area, sewage_quantity, collected_waste, gas_consumption
  # lower part:
    # habitans_per_flats, childs_per_nursery_school, cars
  # pairs:

# only medium-strong correlation
cor.mat[abs(cor.mat)<0.5] <- 0
corrplot(cor.mat, method='square', type='upper', diag=FALSE, tl.cex = 0.6)
(sum(abs(cor.mat)>0)-nrow(cor.mat))/2 # 40

# only strongly correlation
cor.mat[abs(cor.mat)<0.7] <- 0
corrplot(cor.mat, method='square', type='upper', diag=FALSE, tl.cex = 0.6)
(sum(abs(cor.mat)>0)-nrow(cor.mat))/2
  # 7 pairs: big_flats-flat_area, prof_per_stud-childs_per_nursery_school
  # 3 other with nepszamlalas data

# ---- 4.2. PCA analysis ----
pca.analysis <- prcomp(df[,10:54], center=TRUE, scale.=TRUE)
summary(pca.analysis)

# PCA for observed strong correlations
# flat_area, big_flats
flat.pca.analysis <- prcomp(df[,c('flat_area','big_flats')], center=TRUE, scale.=TRUE)
summary(flat.pca.analysis)
df$flat.pca <- flat.pca.analysis$x[,1]
cor(df[,c('flat_area', 'flat.pca')]) # if flat is big --> higher PCA

# childs_per_nursery_school, prof_per_stud
stud.pca.analysis <- prcomp(df[,c('childs_per_nursery_school','prof_per_stud')], center=TRUE, scale.=TRUE)
summary(stud.pca.analysis)
df$stud.pca <- stud.pca.analysis$x[,1]
cor(df[,c('childs_per_nursery_school', 'stud.pca')]) # if lot of childs --> lower PCA

# car, businesses, szja
szja.pca.analysis <- prcomp(df[,c('cars','businesses','szja')], center=TRUE, scale.=TRUE)
summary(szja.pca.analysis)
df$szja.pca <- szja.pca.analysis$x[,1]
cor(df[,c('szja', 'szja.pca')]) # great szja --> higher PCA

# waste_collection, collected_waste
waste.pca.analysis <- prcomp(df[,c("waste_collection", "collected_waste")], center=TRUE, scale.=TRUE)
summary(waste.pca.analysis)
df$waste.pca <- waste.pca.analysis$x[,1] # if lot of waste --> higher PCA

# flat_sewage, sewage_quantity
sewage.pca.analysis <- prcomp(df[,c("flat_sewage", "sewage_quantity")], center=TRUE, scale.=TRUE)
summary(sewage.pca.analysis)
df$sewage.pca <- sewage.pca.analysis$x[,1] # if lot of sewage --> lower PCA

# age0, age10, age20, age30, age40
younger.age.pca.analysis <- prcomp(df[,c('age0','age10','age20','age60','age70')], center=TRUE, scale.=TRUE)
summary(younger.age.pca.analysis)
age.X <- df[,c('age0','age10','age20','age60','age70')]
pca.age.rotated <- psych::principal(age.X, rotate="varimax", nfactors=2, scores=TRUE)
df$age.pca1 <- pca.age.rotated$scores[,1]
df$age.pca2 <- pca.age.rotated$scores[,2]
round(cor(df[,c('age20','age60','age.pca1', 'age.pca2')]),4)
  # lot of age20 --> greater age.pca1
  # lot of age60 --> lower age.pca2

# leaving_exam, uni
uni.pca.analysis <- prcomp(df[,c('leaving_exam','uni')], center=TRUE, scale.=TRUE)
summary(uni.pca.analysis)
df$uni.pca <- uni.pca.analysis$x[,1]
round(cor(df[,c('uni','uni.pca')]),4)
  # lot of uni graduated --> greater uni.pca

# hun, roma
nat.pca.analysis <- prcomp(df[,c('hun', 'roma')], center=TRUE, scale.=TRUE)
summary(nat.pca.analysis)
df$nat.pca <- nat.pca.analysis$x[,1]
round(cor(df[,c('hun', 'roma')]),4)

# ==== 5. Spatial autocorrelation of TISZA support ====
# ---- 5.1. Load NVI data -----
setwd("~/Downloads/egyetem/TDK/magyar_petered_main/ksh_data")

library(ape)
library(data.table)
library(spdep)

params <- readxl::read_xls('ep2024_munis_result.xls',sheet = 'Paraméterek')
counties <- c("baranya","bacs_kisk","bekes","baz","cscs","fejer","gyorms",
       "hajdub","heves","jasznk","komesz","nograd","pest","somogy","szabolcs",
       "tolna","vas","veszprem","zala")
nvi <- data.frame()

for (i in counties){
  county <- readxl::read_xls('ep2024_munis_result.xls',sheet = i)
  nvi <- rbind(nvi, county)
}

colnames(nvi)[1:4] <- c('name','type','voting_places','processing')
colnames(nvi)[5:21] <- params$sign_in_code

# convert to proportion
# keep the general mindset about props
  # turnout = all voters / votable pop
  # party = voters of the party / all voters
# to get the number of the voters of a given party: VP * turnout * voters of the party
nvi$turnout <- nvi$voters/nvi$vox_pop
nvi$tisza <- nvi$tisza/nvi$voters
nvi$fidesz <- nvi$fidesz/nvi$voters
nvi$bal <- nvi$bal/nvi$voters
nvi$other <- rowSums(nvi[,c('memo','lmp','rk2','mmn','mom','jobb','mkkp','mhm')])/nvi$voters
nvi$invalid <- nvi$invalid/nvi$voters

nvi <- nvi[,c('name','vox_pop','turnout','tisza','fidesz','bal','other','invalid'),]
df <- merge(df, nvi, by.x='name', by.y='name', all.x=TRUE)
# writexl::write_xlsx(nvi,'magyar_petered/data/nvi.xlsx')

# ==== 6. Calculate Moran's I ====
locs <- readRDS('locs.rds')
durations <- fread('osrmdurations.csv')
durations <- as.matrix(durations)
rownames(durations) <- colnames(durations) <- locs$NAME
durations <- durations/60 # to minutes
durationsSymm <- (durations + t(durations))/2

c(nrow(durationsSymm), nrow(df))
durationsSymm <- durationsSymm[intersect(colnames(durationsSymm),df$name),]
durationsSymm <- durationsSymm[,intersect(colnames(durationsSymm),df$name)]

weight.matrix <- 1/durationsSymm^2
diag(weight.matrix) <- 0
Moran.I(df$tisza, weight.matrix)$observed # 0.09936719
Moran.I(df$fidesz, weight.matrix)$observed # 0.1093541
Moran.I(df$bal, weight.matrix)$observed # 0.05085588

Moran.I(df$fidesz-df$tisza, weight.matrix)$observed # 0.1086763
Moran.I(df$fidesz-df$bal, weight.matrix)$observed # 0.1086763
Moran.I(df$tisza-df$bal, weight.matrix)$observed # 0.09232332

# lw_dur <- mat2listw(weight.matrix, style = "W", zero.policy = TRUE)
# spdep::moran(df$tisza, lw_dur, n=nrow(df), S0=Szero(lw_dur))$I # 0.09936719

moran.i.tisza <- c()
moran.i.fidesz <- c()
moran.i.bal <- c()
range.min <- seq(1,10,by=0.5)
for (i in range.min){
  range <- dnearneigh(df$point, 0,i, longlat=FALSE)
  dist.matrix <- nb2listw(range, style='W')
  moran.i.tisza <- c(moran.i.tisza,
                      spdep::moran(df$tisza, dist.matrix, n=nrow(df), S0=nrow(df)-1)$I)
  moran.i.fidesz <- c(moran.i.fidesz,
                    spdep::moran(df$fidesz, dist.matrix, n=nrow(df), S0=nrow(df)-1)$I)
  moran.i.bal <- c(moran.i.bal,
                    spdep::moran(df$bal, dist.matrix, n=nrow(df), S0=nrow(df)-1)$I)
}
moran.i.values <- data.frame(tisza=moran.i.tisza, fidesz=moran.i.fidesz, bal=moran.i.bal, 
  range=range.min)

# map each label to a HEX colour with scale_color_manual()
ggplot(moran.i.values, aes(x = range.min)) +
  geom_line(aes(y = moran.i.bal, color = 'DK')) +
  geom_line(aes(y = moran.i.tisza, color = 'TISZA')) +
  geom_line(aes(y = moran.i.fidesz, color = 'Fidesz')) +
  scale_color_manual(
    values = c(
      'DK' = '#007FFF',
      'TISZA' = '#24b574',
      'Fidesz' = '#fd8100'
    ), name = "Party") +
  theme_minimal() +
  labs(
    title = "Spatial autocorrelation of parties' support",
    x = 'Distance range (km)', y = "Moran's I")
# ggsave(filename=paste0(plot.path, '07_spatial_autocorr.png'), 
  #  width = 2000, height = 1500, units = "px", dpi = 300)

# ==== 7. OLS regression without time and distance ====
# ---- 7.0. Preparing for modelling ----
library(boot)

# avoid absolute 0 for modelling
df$tisza <- df$tisza+0.001
df$fidesz <- df$fidesz+0.001
df$bal <- df$bal+0.001

# cross validated r^2
cv.r2 <- function(formula_str, target, data=df){
  df.mod <- data
  set.seed(17)
  df.mod$fold <- sample(1:10,size = nrow(df.mod),replace = TRUE)
  # barplot(table(df.mod$fold))
  RSqr <- rep(NA, 10)

  for (fold in unique(df.mod$fold)) {
    current_ols <- lm(as.formula(formula_str), data = df.mod[df.mod$fold!=fold,])
    pred_y <- predict(current_ols, newdata=df.mod[df.mod$fold==fold,])
    RSqr[fold] <- cor(pred_y, df.mod[[target]][df.mod$fold==fold])^2
  }
  return(RSqr)
}

# ---- 7.1. TISZA regression ----
library(zoo)
library(car)
library(lmtest)

predictors0.tisza <- c(
  "is_mped", "animal_unity", "criminals", "gas_consumption",
  "electricity_consumption", "cultural_programs", "newborns",
  "deaths", "marriages", "net_subs", "small_stores", "estate_area",
  "pensioneers", "migration_diff", "crop_field", "habitans_per_flats",
  "fertility_rate", "len_routes_diff", "flats", "building_permissions", "age90", 
  "lower_elementary", "elementary", "degree", "christian", "ateist", "rel_no_ans",
  "roma_no_ans", "flat.pca", "stud.pca", "szja.pca", "waste.pca", "sewage.pca", "age.pca1", 
  "age.pca1", "age30", "age40","age50", "uni.pca", 'nat.pca',"is_fideszed", "is_dked"
)

formula_str0.tisza <- paste("tisza ~", paste(predictors0.tisza, collapse = " + "))
model0.tisza <- lm(as.formula(formula_str0.tisza), data = df)
summary(model0.tisza)
car::vif(model0.tisza)
bptest(model0.tisza, studentize = TRUE) # heterosked model --> interpret with HC
coeftest(model0.tisza, vcov = hccm(model0.tisza))
model0.tisza.r2 <- cv.r2(formula_str0.tisza, 'tisza')
barplot(model0.tisza.r2,xlab='folds',ylab='R-squared')
mean(model0.tisza.r2)

predictors1.tisza <- c(
  "is_mped", "animal_unity", "criminals", "gas_consumption",
  "electricity_consumption", "cultural_programs", "newborns",
  "deaths", "marriages", "net_subs", "small_stores", "estate_area",
  "pensioneers", "migration_diff", "crop_field", "habitans_per_flats",
  "fertility_rate", "len_routes_diff", "flats", "building_permissions", "age90", 
  "lower_elementary", "elementary", "degree", "christian", "ateist", "rel_no_ans",
  "roma_no_ans", "flat.pca", "stud.pca", "szja.pca", "waste.pca", "sewage.pca", "age.pca1", 
  "age.pca1", "age30", "age40","age50", 'nat.pca',"is_fideszed", "is_dked"
) # without uni.pca

formula_str1.tisza <- paste("tisza ~", paste(predictors1.tisza, collapse = " + "))
model1.tisza <- lm(as.formula(formula_str1.tisza), data = df)
summary(model1.tisza)
car::vif(model1.tisza)
bptest(model1.tisza, studentize = TRUE) # heterosked model --> interpret with HC
coeftest(model1.tisza, vcov = hccm(model1.tisza))
model1.tisza.r2 <- cv.r2(formula_str1.tisza, 'tisza')
barplot(model1.tisza.r2,xlab='folds',ylab='R-squared')
mean(model1.tisza.r2)

# oselmeny a konzervativizmusrol
# nagykovetek bekeretese
# lazar vagy navracsics

predictors2.tisza <- c(
  "is_mped", "electricity_consumption", "cultural_programs", "estate_area", 
  "flats", "lower_elementary", "elementary", "degree", "christian", "ateist",
   "stud.pca", "szja.pca", "waste.pca", "sewage.pca", "age40", 'nat.pca', 'is_dked'
) # dropped non-significant variables
formula_str2.tisza <- paste("tisza ~", paste(predictors2.tisza, collapse = " + "))
model2.tisza <- lm(as.formula(formula_str2.tisza), data = df)
summary(model2.tisza)
car::vif(model2.tisza)
bptest(model2.tisza, studentize = TRUE)
coeftest(model2.tisza, vcov = hccm(model2.tisza))
model2.tisza.r2 <- cv.r2(formula_str2.tisza, 'tisza')
barplot(model2.tisza.r2,xlab='folds',ylab='R-squared')
mean(model2.tisza.r2)

predictors3.tisza <- c(
  "is_mped", "estate_area", "lower_elementary", "elementary", "degree", "christian",
  "ateist", "stud.pca", "szja.pca", "sewage.pca"
) # only signif from model2
formula_str3.tisza <- paste("tisza ~", paste(predictors3.tisza, collapse = " + "))
model3.tisza <- lm(as.formula(formula_str3.tisza), data = df)
summary(model3.tisza)
car::vif(model3.tisza) # all good, nothing above 5
bptest(model3.tisza, studentize = TRUE) # heterosked --> HC
coeftest(model3.tisza, vcov = hccm(model3.tisza))
mean(cv.r2(formula_str3.tisza, 'tisza'))
model3.tisza.r2 <- cv.r2(formula_str3.tisza, 'tisza')
barplot(model3.tisza.r2,xlab='folds',ylab='R-squared')
mean(model3.tisza.r2)

ic.tisza <- data.frame(
  name=c('model0.tisza', 'model1.tisza', 'model2.tisza', 'model3.tisza'),
  AIC=AIC(model0.tisza, model1.tisza, model2.tisza, model3.tisza)$AIC,
  BIC=BIC(model0.tisza, model1.tisza, model2.tisza, model3.tisza)$BIC
)

ggplot(ic.tisza, aes(x=name, y=AIC)) +
  geom_col() + coord_cartesian(ylim = c(min(ic.tisza$AIC), max(ic.tisza$AIC)*0.9)) +
  theme_minimal() +
  theme(legend.position='bottom',) +
  labs(title = 'AIC Modelleredmenyek', subtitle = 'legkisebb a legjobb') +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
  plot.subtitle = element_text(hjust = 0.5))

ggplot(ic.tisza, aes(x=name, y=BIC)) +
  geom_col() + coord_cartesian(ylim = c(min(ic.tisza$BIC), max(ic.tisza$BIC)*0.9)) +
  theme_minimal() +
  theme(legend.position='bottom',) +
  labs(title = 'BIC Modelleredmenyek', subtitle = 'legkisebb a legjobb') +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
  plot.subtitle = element_text(hjust = 0.5))

corrplot(cor(as.data.frame(df[,predictors2.tisza])), method='square', type='upper', diag=FALSE, tl.cex = 0.6)
cor(data.frame(df$szja.pca, df$uni.pca))

set.seed(17)
boot.results.tisza <- Boot(model2.tisza, R = 1000, ncores=4)
summary(boot.results.tisza)
hist(boot.results.tisza, layout = c(2, 3))
plot(model2.tisza, which=1)
cv.r2(formula_str2.tisza, 'tisza')

# ---- 7.2. FIDESZ regression ----
predictors1.fidesz <- c(
  "is_fideszed", "animal_unity", "criminals", "gas_consumption",
  "electricity_consumption", "cultural_programs", "newborns",
  "deaths", "marriages", "net_subs", "small_stores", "estate_area",
  "pensioneers", "migration_diff", "crop_field", "habitans_per_flats",
  "fertility_rate", "len_routes_diff", "flats", "building_permissions", "age90", 
  "lower_elementary", "elementary", "degree", "christian", "ateist", "rel_no_ans",
  "roma_no_ans", "flat.pca", "stud.pca", "szja.pca", "waste.pca", "sewage.pca", "age.pca1", 
  "age.pca1", "age30", "age40","age50", 'nat.pca',"is_mped", "is_dked"
) # drop uni.pca because of same reasons

formula_str1.fidesz <- paste("fidesz ~", paste(predictors1.fidesz, collapse = " + "))
model1.fidesz <- lm(as.formula(formula_str1.fidesz), data = df)
summary(model1.fidesz)
car::vif(model1.fidesz)
bptest(model1.fidesz, studentize = TRUE) # heterosked --> HC
coeftest(model1.fidesz, vcov = hccm(model1.fidesz))
model1.fidesz.r2 <- cv.r2(formula_str1.fidesz, 'fidesz')
barplot(model1.fidesz.r2,xlab='folds',ylab='R-squared')
mean(model1.fidesz.r2)

predictors2.fidesz <- c(
  "is_fideszed", "gas_consumption",
  "electricity_consumption", "net_subs", "pensioneers", 
  "crop_field", "habitans_per_flats", "lower_elementary", "elementary", "degree", "christian",
  "ateist", "rel_no_ans", "stud.pca", "szja.pca", "sewage.pca", "nat.pca", "is_dked"
) # dropped insignif variables

formula_str2.fidesz <- paste("fidesz ~", paste(predictors2.fidesz, collapse = " + "))
model2.fidesz <- lm(as.formula(formula_str2.fidesz), data = df)
summary(model2.fidesz)
car::vif(model2.fidesz)
bptest(model2.fidesz, studentize = TRUE) # heterosked --> HC
coeftest(model2.fidesz, vcov = hccm(model2.fidesz))
model2.fidesz.r2 <- cv.r2(formula_str2.fidesz, 'fidesz')
barplot(model2.fidesz.r2,xlab='folds',ylab='R-squared')
mean(model2.fidesz.r2)

ic.fidesz <- data.frame(
  name=c('model1.fidesz', 'model2.fidesz'),
  AIC=AIC(model1.fidesz, model2.fidesz)$AIC,
  BIC=BIC(model1.fidesz, model2.fidesz)$BIC
)

ggplot(ic.fidesz, aes(x=name, y=AIC)) +
  geom_col() + coord_cartesian(ylim = c(min(ic.fidesz$AIC), max(ic.fidesz$AIC)*0.99)) +
  theme_minimal() +
  theme(legend.position='bottom',) +
  labs(title = 'AIC Modelleredmenyek', subtitle = 'legkisebb a legjobb') +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
  plot.subtitle = element_text(hjust = 0.5))

ggplot(ic.fidesz, aes(x=name, y=BIC)) +
  geom_col() + coord_cartesian(ylim = c(min(ic.fidesz$BIC), max(ic.fidesz$BIC)*0.99)) +
  theme_minimal() +
  theme(legend.position='bottom',) +
  labs(title = 'BIC Modelleredmenyek', subtitle = 'legkisebb a legjobb') +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
  plot.subtitle = element_text(hjust = 0.5))

corrplot(cor(as.data.frame(df[,predictors2.fidesz])), method='square', type='upper', diag=FALSE, tl.cex = 0.6)

set.seed(17)
boot.results.fidesz <- Boot(model2.fidesz, R = 1000, ncores=4)
summary(boot.results.fidesz)
hist(boot.results.fidesz, layout = c(2, 3))
plot(model2.fidesz, which=1)
cv.r2(formula_str2.fidesz, 'fidesz')

# ---- 7.3. DK regression ----
predictors0.bal <- c(
  "is_dked", "animal_unity", "criminals", "gas_consumption",
  "electricity_consumption", "cultural_programs", "newborns",
  "deaths", "marriages", "net_subs", "small_stores", "estate_area",
  "pensioneers", "migration_diff", "crop_field", "habitans_per_flats",
  "fertility_rate", "len_routes_diff", "flats",
  "building_permissions", "age90", "lower_elementary", "elementary",
  "degree", "flat.pca", "stud.pca", "szja.pca", "waste.pca", "sewage.pca",
  "age.pca1", "age.pca2", "age30", "age40","age50", "uni.pca", "is_mped",
  "is_fideszed"
)

formula_str0.bal <- paste("bal ~", paste(predictors0.bal, collapse = " + "))
model0.bal <- lm(as.formula(formula_str0.bal), data = df)
summary(model0.bal)
car::vif(model0.bal)
bptest(model0.bal, studentize = TRUE) # heterosked --> HC
 # coeftest(model0.bal, vcov = hccm(model0.bal)) # fails because of uni.pca
model0.bal.r2 <- cv.r2(formula_str0.bal, 'fidesz')
barplot(model0.bal.r2,xlab='folds',ylab='R-squared')
mean(model0.bal.r2)

predictors1.bal <- c(
  "is_dked", "animal_unity", "criminals", "gas_consumption",
  "electricity_consumption", "cultural_programs", "newborns",
  "deaths", "marriages", "net_subs", "small_stores", "estate_area",
  "pensioneers", "migration_diff", "crop_field", "habitans_per_flats",
  "fertility_rate", "len_routes_diff", "flats", "building_permissions", "age90", 
  "lower_elementary", "elementary", "degree", "christian", "ateist", "rel_no_ans",
  "roma_no_ans", "flat.pca", "stud.pca", "szja.pca", "waste.pca", "sewage.pca", "age.pca1", 
  "age.pca1", "age30", "age40","age50", 'nat.pca',"is_fideszed", "is_mped"
)
formula_str1.bal <- paste("bal ~", paste(predictors1.bal, collapse = " + "))
model1.bal <- lm(as.formula(formula_str1.bal), data = df)
summary(model1.bal)
car::vif(model1.bal)
bptest(model1.bal, studentize = TRUE) # heterosked --> HC
coeftest(model1.bal, vcov = hccm(model1.bal))
model1.bal.r2 <- cv.r2(formula_str1.bal, 'bal')
barplot(model1.bal.r2,xlab='folds',ylab='R-squared')
mean(model1.bal.r2)

predictors2.bal <- c(
  "is_dked", "animal_unity", "estate_area",
  "pensioneers", "crop_field", "habitans_per_flats", "len_routes_diff", "lower_elementary",
  "elementary", "degree", "christian", "ateist", "rel_no_ans", "stud.pca", "sewage.pca",
  "age.pca1", "age40", "age50", "flat.pca", "stud.pca", "age.pca1",
  "age.pca2","age40", 'age50', "is_mped"
) # drop insignificant variables

formula_str2.bal <- paste("bal ~", paste(predictors2.bal, collapse = " + "))
model2.bal <- lm(as.formula(formula_str2.bal), data = df)
summary(model2.bal)
car::vif(model2.bal)
bptest(model2.bal, studentize = TRUE) # heterosked --> HC
coeftest(model2.bal, vcov = hccm(model2.bal))
model2.bal.r2 <- cv.r2(formula_str2.bal, 'bal')
barplot(model2.bal.r2,xlab='folds',ylab='R-squared')
mean(model2.bal.r2)

predictors3.bal <- c(
  "is_dked", "animal_unity",
  "pensioneers", "crop_field", "habitans_per_flats", "ateist", "rel_no_ans", "stud.pca"
  , "stud.pca", "sewage.pca", "age.pca1", "age40", "age.pca2", "is_mped"
) # keep only significant or close-to-signif variables

formula_str3.bal <- paste("bal ~", paste(predictors3.bal, collapse = " + "))
model3.bal <- lm(as.formula(formula_str3.bal), data = df)
summary(model3.bal)
car::vif(model3.bal)
bptest(model3.bal, studentize = TRUE) # heterosked --> HC
coeftest(model3.bal, vcov = hccm(model3.bal))
model3.bal.r2 <- cv.r2(formula_str3.bal, 'bal')
barplot(model3.bal.r2,xlab='folds',ylab='R-squared')
mean(model3.bal.r2)

ic.bal <- data.frame(
  name=c('model1.bal', 'model2.bal', 'model3.bal'),
  AIC=AIC(model1.bal, model2.bal, model3.bal)$AIC,
  BIC=BIC(model1.bal, model2.bal, model3.bal)$BIC
)

ggplot(ic.bal, aes(x=name, y=AIC)) +
  geom_col() + coord_cartesian(ylim = c(min(ic.bal$AIC), max(ic.bal$AIC)*0.99)) +
  theme_minimal() +
  theme(legend.position='bottom',) +
  labs(title = 'AIC Modelleredmenyek', subtitle = 'legkisebb a legjobb') +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
  plot.subtitle = element_text(hjust = 0.5))

ggplot(ic.bal, aes(x=name, y=BIC)) +
  geom_col() + coord_cartesian(ylim = c(min(ic.bal$BIC), max(ic.bal$BIC)*0.99)) +
  theme_minimal() +
  theme(legend.position='bottom',) +
  labs(title = 'BIC Modelleredmenyek', subtitle = 'legkisebb a legjobb') +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
  plot.subtitle = element_text(hjust = 0.5))

# winner: model3 but model2 is close to it, but be careful with overfitting

# ==== 8. Analysis of the distance to the closest campaign city ====
# ---- 8.1.0. TISZA preparation ----
mped.cities <- df[df$is_mped==T,'name']
mped <- durationsSymm[,mped.cities]
c(nrow(mped), ncol(mped)) # 3153x184 matrix
  # all muni in rows
  # only mped rows in columns --> look up for the smallest!

fidesz.cities <- df[df$is_fideszed==T,'name']
fideszed <- durationsSymm[,fidesz.cities]
c(nrow(fideszed), ncol(fideszed))

dk.cities <- df[df$is_dked==T,'name']
dked <- durationsSymm[,dk.cities]
c(nrow(dked), ncol(dked))

# add tisza to variables name with llm
# iterate throught the matrix to get the value of the closest city
closest.cities <- c()
closest.id <- c()
closest.distances <- c()
closest.tisza <- c()

for(i in 1:nrow(df)){
  # save the current city name
  current.city <- df[i,'name']

  # save the closest city name
  closest.city <- which.min(mped[df[i,'name'],])
  closest.city <- rownames(as.matrix(closest.city))
  closest.cities <- c(closest.cities, closest.city)

  # save the result of the closest city
  current.result <- df[df$name==closest.city,'tisza']
  closest.tisza <- c(closest.tisza, current.result)

  # save the distance to the closest mp city
  closest.distance <- mped[current.city,closest.city]
  closest.distances <- c(closest.distances, closest.distance)
}

df$tisza.closest.city <- closest.cities
df$tisza.closest.distance <- closest.distances
df$tisza.closest.result <- closest.tisza
df$tisza.diff.from.closest <- df$tisza-df$tisza.closest.result

# ---- 8.1.1. TISZA plots ----
hist(df$tisza.closest.distance) # need log1p!
hist(log1p(df[df$tisza.closest.distance,'tisza.closest.distance'])) # outliers at 0

ggplot(df, aes(x=tisza.closest.distance, y=tisza)) +
  geom_point() +
  geom_smooth(method = "lm", col = "red") +
  geom_smooth(method = "loess", col = "blue")

ggplot(df, aes(x=log1p(tisza.closest.distance), y=tisza)) +
  geom_point() +
  geom_smooth(method = "lm", col = "red") +
  geom_smooth(method = "loess", col = "blue")

df$tisza.closest.log1p <- log1p(df$tisza.closest.distance)

# add a cap to the logged lower outliers (mped cities)
boxplot(df$tisza.closest.distance)
boxplot(df$tisza.closest.log1p)
lower.tukey <- quantile(log1p(df$tisza.closest.distance))[2]-(quantile(log1p(df$tisza.closest.distance))[4]-quantile(log1p(df$tisza.closest.distance))[2])*1.5
df[df$tisza.closest.log1p<lower.tukey,'tisza.closest.log1p'] <- lower.tukey

ggplot(df, aes(x=tisza.closest.distance, y=tisza)) +
  geom_point() +
  geom_smooth(method = "lm", col = "red") +
  geom_smooth(method = "loess", col = "blue")

ggplot(df, aes(x=tisza.closest.log1p, y=tisza)) +
  geom_point() +
  geom_smooth(method = "lm", col = "red") +
  geom_smooth(method = "loess", col = "blue")
# now lm and loess are close to each other

summary(lm(tisza~tisza.closest.distance, data=df))$r.squared
summary(lm(tisza~log1p(tisza.closest.distance), data=df))$r.squared
summary(loess(tisza~log1p(tisza.closest.distance), data=df))

# miket erdemes modellezni
  # nincs closest.mp, dummy-val
  # closest.mp linearisan
  # closest.mp logolva
  # closest.mp negyzetesen
  # closest.mp logolva es log negyzetesen
  # osszehasonlitas: IC es cv.r2

# ==== 9. OLS regression with distance ====
# ---- 9. TISZA regression -----
predictors4.tisza <- c(
  "tisza.closest.distance", "electricity_consumption", "cultural_programs", "estate_area", "crop_field", 
  "flats", "lower_elementary", "elementary", "degree",
   "stud.pca", "szja.pca", "waste.pca", "sewage.pca", "age40", 'is_dked'
) # dropped non-significant variables
formula_str4.tisza <- paste("tisza ~", paste(predictors4.tisza, collapse = " + "))
model4.tisza <- lm(as.formula(formula_str4.tisza), data = df)
summary(model4.tisza)
car::vif(model4.tisza)
bptest(model4.tisza, studentize = TRUE)
coeftest(model4.tisza, vcov = hccm(model4.tisza))
model4.tisza.r2 <- cv.r2(formula_str4.tisza, 'tisza', df)
barplot(model4.tisza.r2,xlab='folds',ylab='R-squared')
mean(model4.tisza.r2)

BIC(model4.tisza, model2.tisza)

predictors5.tisza <- c(
  "log1p(tisza.closest.distance)", "electricity_consumption", "cultural_programs", "estate_area", "crop_field", 
  "flats", "lower_elementary", "elementary", "degree",
   "stud.pca", "szja.pca", "waste.pca", "sewage.pca", "age40", 'is_dked'
) # dropped non-significant variables
formula_str5.tisza <- paste("tisza ~", paste(predictors5.tisza, collapse = " + "))
model5.tisza <- lm(as.formula(formula_str5.tisza), data = df)
summary(model5.tisza)
car::vif(model5.tisza)
bptest(model5.tisza, studentize = TRUE)
coeftest(model5.tisza, vcov = hccm(model5.tisza))
model5.tisza.r2 <- cv.r2(formula_str5.tisza, 'tisza', df)
barplot(model5.tisza.r2,xlab='folds',ylab='R-squared')
mean(model5.tisza.r2)

BIC(model5.tisza, model2.tisza)

# todelete later ----

# with st_nearest_feature
labeled.munis <- st_as_sf(df[df$is_mped==T,c('id', 'name', 'point')], crs=4326)
point.id <- st_as_sf(df[,c('id','name','point')], crs=4326)
nearest_mp <- st_nearest_feature(point.id, labeled.munis)
nearest_mp <- labeled.munis[nearest_mp, c('name', 'id')]
colnames(nearest_mp) <- c('nearest_mp.name', 'nearest_mp.id','nearest_mp.point')
st_geometry(nearest_mp) <- "nearest_mp.point"

# ==== 9. OLS regression with time ====

