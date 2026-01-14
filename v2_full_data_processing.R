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

# ==== 2. Read-in settlements affected by the campaign ====
library(ggplot2)

campaign <- readxl::read_excel('magyar_petered_telepulesek.xlsx')

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
df$x <- st_coordinates(points)[,1]
df$y <- st_coordinates(points)[,2]

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

# ---- Load 28 files from 1_jo (Timea data) ----
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
df <- merge(df, routes, by.x='id', by.y='ELEM_KOD')
df <- merge(df, flats, by.x='id', by.y='ELEM_KOD')
df <- merge(df, buildings, by.x='id', by.y='ELEM_KOD')

# --- Load detailed age and education data from Nepszamlalas 2022 ----
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

# Set up column names
setwd("/Users/mac/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data")
desc <- readxl::read_excel('ksh_data_concated.xlsx', sheet = 'description')
desc$description <- stri_trans_nfc(desc$description)
csvin <- desc[grepl('.csv',desc$description),]
csvin$description <- substr(csvin$description, 1, nchar(csvin$description) - 4)
desc[csvin$name==desc$name,]$description <- csvin$description

colnames(df)[10:54] <- desc$name
df[is.na(df)] <- 0

# ==== 6. Plot data ====
# can be found in plot_ksh_data.R

# ==== 7. Descriptive statistic of timea ====
# --- 7.1. Get frequency table ----
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

# ---- 7.2. Add geometry to the values table ----
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
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal() +
  theme(legend.position='bottom') +
  labs(title = 'A kiválasztott adatokban legmagasabb \n értékekkel rendelkező települések', ) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
        axis.title = element_blank(), axis.text=element_blank(), panel.grid=element_blank())

# ==== 8. PCA and correlations ====
# ---- 8.1. Correlation matrix ----
library(corrplot)

# apply logs and save to stat table

for (i in desc$name){
  if (desc[desc$name==i,'using_log1p']==1) {df[[i]] <- log1p(df[[i]])}
}

cor.mat <- cor(as.data.frame(df[,10:54]))
corrplot(cor(as.data.frame(df[,10:54])), method='square', type='upper', diag=FALSE, tl.cex = 0.6)

# only medium and strong correlation
cor.mat[abs(cor.mat)<0.3] <- 0
corrplot(cor.mat, method='square', type='upper', diag=FALSE, tl.cex = 0.6)
(sum(abs(cor.mat)>0)-nrow(cor.mat))/2 # 58
# easy to interpret the similar values
  # upper-left corner: 
    # flat_sewage, szja, flat_area, sewage_quantity, collected_waste, gas_consumption
  # lower part:
    # habitans_per_flats, childs_per_nursery_school, cars
  # pairs:

# only medium-strong correlation
cor.mat[abs(cor.mat)<0.5] <- 0
corrplot(cor.mat, method='square', type='upper', diag=FALSE, tl.cex = 0.6)
(sum(abs(cor.mat)>0)-nrow(cor.mat))/2 # 37

# only strongly correlation
cor.mat[abs(cor.mat)<0.7] <- 0
corrplot(cor.mat, method='square', type='upper', diag=FALSE, tl.cex = 0.6)
(sum(abs(cor.mat)>0)-nrow(cor.mat))/2
  # 2 pairs: big_flats-flat_area, prof_per_stud-childs_per_nursery_school
  # 3 other with nepszamlalas data

# ---- 8.2. PCA analysis ----
pca.analysis <- prcomp(df[,10:54], center=TRUE, scale.=TRUE)
summary(pca.analysis)

# PCA for observed strong correlations
# flat_area, big_flats
flat.pca.analysis <- prcomp(df[,c('flat_area','big_flats')], center=TRUE, scale.=TRUE)
summary(flat.pca.analysis)
df$flat.pca <- flat.pca.analysis$x[,1]

# childs_per_nursery_school, prof_per_stud
stud.pca.analysis <- prcomp(df[,c('childs_per_nursery_school','prof_per_stud')], center=TRUE, scale.=TRUE)
summary(stud.pca.analysis)
df$stud.pca <- stud.pca.analysis$x[,1]

# car, businesses, szja
szja.pca.analysis <- prcomp(df[,c('cars','businesses','szja')], center=TRUE, scale.=TRUE)
summary(szja.pca.analysis)
df$szja.pca <- szja.pca.analysis$x[,1]

# age0, age10, age20, age30, age40
younger.age.pca.analysis <- prcomp(df[,c('age0','age10','age20','age60','age70')], center=TRUE, scale.=TRUE)
summary(younger.age.pca.analysis)
age.X <- df[,c('age0','age10','age20','age60','age70')]
pca.age.rotated <- psych::principal(age.X, rotate="varimax", nfactors=2, scores=TRUE)
df$age.pca1 <- pca.age.rotated$scores[,1]
df$age.pca2 <- pca.age.rotated$scores[,2]

# leaving_exam, uni
uni.pca.analysis <- prcomp(df[,c('leaving_exam','uni')], center=TRUE, scale.=TRUE)
summary(uni.pca.analysis)
df$uni.pca <- uni.pca.analysis$x[,1]

col.names <- c("name", "id", "is_mped", "is_fideszed", "is_dked", "geometry",
  "point", "x", "y", "animal_unity", "big_flats", "waste_collection",
  "flat_sewage", "criminals", "szja", "flat_area", "sewage_quantity",
  "prof_per_stud", "collected_waste", "gas_consumption",
  "electricity_consumption", "cultural_programs", "newborns",
  "businesses", "deaths", "marriages", "net_subs", "small_stores",
  "estate_area", "pensioneers", "migration_diff", "crop_field",
  "habitans_per_flats", "childs_per_nursery_school", "cars",
  "fertility_rate", "len_routes_diff", "flats",
  "building_permissions", "age0", "age10", "age20", "age30", "age40",
  "age50", "age60", "age70", "age80", "age90", "lower_elementary",
  "elementary", "degree", "leaving_exam", "uni", "pop", "vox_pop",
  "turnout", "tisza", "fidesz", "bal", "other", "invalid",
  "flat.pca", "stud.pca", "szja.pca", 'age.pca1', 'age.pca2', 'age30', 'age40', 'age50',
   "uni.pca")

exclude_idx <- match(c("flat_area", "big_flats", "childs_per_nursery_school", 
  "prof_per_stud", "cars", "businesses", "szja", "age0", "age10", "age20", "age60", "age70", "age80", "leaving_exam", "uni", 'pop'), colnames(df))
exclude_idx <- exclude_idx[!is.na(exclude_idx)]
select_idx <- setdiff(10:ncol(df), exclude_idx)

# ==== 9. Spatial autocorrelation of TISZA support ====
# ----- 9.1. Load NVI data -----
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

# ----- 9.2. Calculate Moran's I -----
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

# ==== 10. OLS regression without time ====
# ---- 10.1. TISZA regression ----
predictors1.tisza <- c(
  "is_mped", "animal_unity", "waste_collection", "flat_sewage", "criminals",
  "sewage_quantity", "collected_waste", "gas_consumption",
  "electricity_consumption", "cultural_programs", "newborns",
  "deaths", "marriages", "net_subs", "small_stores", "estate_area",
  "pensioneers", "migration_diff", "crop_field", "habitans_per_flats",
  "fertility_rate", "len_routes_diff", "flats",
  "building_permissions", "age90", "lower_elementary", "elementary",
  "degree", "flat.pca", "stud.pca", "szja.pca",
  "age.pca1", "age.pca1", "age30", "age40","age50", "uni.pca", "is_fideszed",
  "is_dked", "tisza"
)

formula_str1.tisza <- paste("tisza ~", paste(predictors1.tisza, collapse = " + "))
model1.tisza <- lm(as.formula(formula_str1.tisza), data = df)
summary(model1.tisza)
car::vif(model1.tisza)

predictors2.tisza <- c(
  "is_mped", "flat_sewage", "criminals", "collected_waste", "gas_consumption",
  "electricity_consumption", "cultural_programs", "estate_area", "habitans_per_flats", "flats",
   "lower_elementary", "elementary", "degree", "flat.pca", "stud.pca", "szja.pca", "age.0_40.pca2",
  "age.50plus.pca", "uni.pca"
) # dropped non-significant variables
formula_str2.tisza <- paste("tisza ~", paste(predictors2.tisza, collapse = " + "))
model2.tisza <- lm(as.formula(formula_str2.tisza), data = df)
summary(model2.tisza)
car::vif(model2.tisza) # uni.pca: 45.165025 --> how???
cor(data.frame(df$szja.pca, df$uni.pca)) # 0.7411074 strong correlation with szja.pca!

predictors3.tisza <- c(
  "is_mped", "flat_sewage", "criminals", "collected_waste", "gas_consumption",
  "electricity_consumption", "cultural_programs", "estate_area", "habitans_per_flats", "flats",
   "lower_elementary", "elementary", "degree", "flat.pca", "stud.pca", "szja.pca", "age.0_40.pca2",
  "age.50plus.pca"
) # dropped uni.pca
formula_str3.tisza <- paste("tisza ~", paste(predictors3.tisza, collapse = " + "))
model3.tisza <- lm(as.formula(formula_str3.tisza), data = df)
summary(model3.tisza)
car::vif(model3.tisza) # all good, nothing above 5

BIC(model1.tisza, model2.tisza, model3.tisza)

corrplot(cor(as.data.frame(df[,predictors2.tisza])), method='square', type='upper', diag=FALSE, tl.cex = 0.6)
cor(data.frame(df$szja.pca, df$uni.pca))

# ---- 10.2. FIDESZ regression ----
predictors1.fidesz <- c(
  "is_fideszed", "animal_unity", "waste_collection", "flat_sewage", "criminals",
  "sewage_quantity", "collected_waste", "gas_consumption",
  "electricity_consumption", "cultural_programs", "newborns",
  "deaths", "marriages", "net_subs", "small_stores", "estate_area",
  "pensioneers", "migration_diff", "crop_field", "habitans_per_flats",
  "fertility_rate", "len_routes_diff", "flats",
  "building_permissions", "age90", "lower_elementary", "elementary",
  "degree", "flat.pca", "stud.pca", "szja.pca",
  "age.pca1", "age.pca1", "age30", "age40","age50", "uni.pca", "is_mped",
  "is_dked"
)

formula_str1.fidesz <- paste("fidesz ~", paste(predictors1.fidesz, collapse = " + "))
model1.fidesz <- lm(as.formula(formula_str1.fidesz), data = df)
summary(model1.fidesz)
car::vif(model1.fidesz)

predictors2.fidesz <- c(
  "is_fideszed", "waste_collection", "collected_waste", "gas_consumption",
  "electricity_consumption", "net_subs", "pensioneers", "migration_diff", 
  "crop_field", "habitans_per_flats", "flat.pca", "stud.pca", "szja.pca",
  "age.0_40.pca2", "uni.pca", "is_dked"
)

formula_str2.fidesz <- paste("fidesz ~", paste(predictors2.fidesz, collapse = " + "))
model2.fidesz <- lm(as.formula(formula_str2.fidesz), data = df)
summary(model2.fidesz)
car::vif(model2.fidesz)

BIC(model1.fidesz, model2.fidesz) # model2 better

# ---- 10.3. DK regression ----
predictors1.bal <- c(
  "is_dked", "animal_unity", "waste_collection", "flat_sewage", "criminals",
  "sewage_quantity", "collected_waste", "gas_consumption",
  "electricity_consumption", "cultural_programs", "newborns",
  "deaths", "marriages", "net_subs", "small_stores", "estate_area",
  "pensioneers", "migration_diff", "crop_field", "habitans_per_flats",
  "fertility_rate", "len_routes_diff", "flats",
  "building_permissions", "age90", "lower_elementary", "elementary",
  "degree", "flat.pca", "stud.pca", "szja.pca",
  "age.pca1", "age.pca2", "age30", "age40","age50", "uni.pca", "is_mped",
  "is_fideszed"
)

formula_str1.bal <- paste("bal ~", paste(predictors1.bal, collapse = " + "))
model1.bal <- lm(as.formula(formula_str1.bal), data = df)
summary(model1.bal)
car::vif(model1.bal)

predictors2.bal <- c(
  "is_dked", "animal_unity", "collected_waste",
  "deaths", "net_subs", "estate_area",
  "pensioneers", "crop_field", "habitans_per_flats",
  "lower_elementary", "elementary",
  "degree", "flat.pca", "stud.pca",
  "age.50plus.pca", "uni.pca", "is_mped"
)

formula_str2.bal <- paste("bal ~", paste(predictors2.bal, collapse = " + "))
model2.bal <- lm(as.formula(formula_str2.bal), data = df)
summary(model2.bal)
car::vif(model2.bal)

# heteroskedasticity test
# ertelmezesek, hogy mi volt logolva, mi nem
# pca hol negativ-pozitiv
# modellek r-negyzete keresztvalidacioval (stabilitas teszt)

# ==== 12. OLS regression with time ====

