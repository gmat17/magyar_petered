setwd("~/Downloads/egyetem/TDK/magyar_petered_main")

# ---- Read-in necessary files ----
library(sf)
library(leaflet)
library(stringi)

# Edit NVI table
# The source table is downloadable from the valasztas.hu page, 
# this is the result table of the 2024 EP-elections
params <- readxl::read_xls('ksh_data/ep2024_munis_result.xls',sheet = 'Paraméterek')

counties <- c("baranya","bacs_kisk","bekes","baz","cscs","fejer","gyorms",
       "hajdub","heves","jasznk","komesz","nograd","pest","somogy","szabolcs",
       "tolna","vas","veszprem","zala")
nvi <- data.frame()

for (i in counties){
  county <- readxl::read_xls('ksh_data/ep2024_munis_result.xls',sheet = i)
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
# writexl::write_xlsx(nvi,'magyar_petered/data/nvi.xlsx')

setwd("~/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data")
# load munis' shapefile
shape_df <- st_read('kozighatarok/admin8.shp')
shape_df <- st_make_valid(shape_df)
shape_df <- st_transform(shape_df, crs = 4326)
# attention! different number of rows!
c(nrow(shape_df), nrow(nvi))
tbl <- table(shape_df$NAME)
shape_df[shape_df$NAME %in% rownames(tbl[tbl>1]),]
  # multiple rows <-- different geometry
max(tbl[tbl>1]) # maximum duple, not triple or above
  # 40 cases: duplicate rows

# eliminate duplicates
dupl.table <- shape_df[shape_df$NAME %in% rownames(tbl[tbl>1]),]
dupl.table <- data.frame('index'=rownames(dupl.table), 'name'=dupl.table$NAME)
is.added.name <- c()
is.added.index <- c()
for(i in dupl.table$index){
  if(!dupl.table[dupl.table$index==i,'name'] %in% is.added.name){
    is.added.name <- c(dupl.table[dupl.table$index==i,'name'], is.added.name)
    is.added.index <- c(i, is.added.index)
  }
}
non.index <- as.numeric(setdiff(dupl.table$index, is.added.index))
shape_df <- shape_df[-non.index,]

# load back to nvi
nvi <- merge(nvi, shape_df[,c('NAME', 'geometry')], by.x='name', by.y='NAME')
nvi[nvi$name %in% rownames(table(nvi$name)[table(nvi$name)>1]),]

nvi <- st_as_sf(nvi)

# load hungary's shape
hun_shape <- st_read('kozighatarok/admin2.shp')
hun_shape <- st_make_valid(hun_shape)
hun_shape <- st_transform(hun_shape, crs = 4326)

# ---- Spatial-autoregression of TISZA ----
# general plot about the support
library(ggplot2)
library(ape)

ggplot(nvi) +
  geom_sf(data=hun_shape, fill='white', size=0.3,color='black') +
  geom_sf(aes(fill = tisza)) +
  scale_fill_gradient(low='white',high='#24b574') +
  theme_minimal() +
  theme(legend.position='bottom') +
  labs(title = 'A TISZA párt támogatottsága egyes\n településeken a 2024-es EP választáson',
       fill = 'szavazati arány') +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
        axis.title = element_blank(), axis.text=element_blank(), panel.grid=element_blank()) +
  guides(fill = guide_colorbar(barwidth = 10, barheight = 0.5))

# call the dur matrix
library(data.table)
setwd("~/Downloads/egyetem/TDK/magyar_petered_main")
locs <- readRDS('ksh_data/locs.rds')
durations <- fread('ksh_data/osrmdurations.csv')
durations <- as.matrix(durations)
rownames(durations) <- colnames(durations) <- locs$NAME
durations <- durations/60/60
durationsSymm <- (durations + t(durations))/2
durationsSymm

c(nrow(durationsSymm), nrow(nvi))
durationsSymm <- durationsSymm[intersect(colnames(durationsSymm),nvi$name),]
durationsSymm <- durationsSymm[,intersect(colnames(durationsSymm),nvi$name)]

weight.matrix <- 1/durationsSymm^2
diag(weight.matrix) <- 0
Moran.I(nvi$tisza, weight.matrix)

# dnearneigh(sp::coordinates(teradat[,7:8]), 0,40, longlat=TRUE) --> ezt megcsinalni
# fidesz-tisza es tobbi parositas kulonbozet --> arra Moran's I
  # ima, hogy ne legyen
# utana OLS regressio --> tobbfele dummy
# orszagjarastol eltelt ido
  # valasztasig eltelt napok szama
  # days_to_election valtozo --> csak 130 lesz --> ahol nincs: vmi jo nagy szam
    # interakcios tag a magyar peter dummyval --> 0 kinullazza