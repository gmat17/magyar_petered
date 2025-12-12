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

# convert to probabilties
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

writexl::write_xlsx(nvi,'magyar_petered/data/nvi.xlsx')

setwd("~/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data")
# load munis' shapefile
shape_df <- st_read('kozighatarok/admin8.shp')
shape_df <- st_make_valid(shape_df)
shape_df <- st_transform(shape_df, crs = 4326)
nvi <- merge(nvi, shape_df[,c('NAME', 'geometry')], by.x='name', by.y='NAME')
nvi <- st_as_sf(nvi)

# load hungary's shape
hun_shape <- st_read('kozighatarok/admin2.shp')
hun_shape <- st_make_valid(hun_shape)
hun_shape <- st_transform(hun_shape, crs = 4326)

# ---- Spatial-autoregression of TISZA ----
# general plot about the support
library(ggplot2)

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

# call the matrixes
library(sp)
library(spdep)
library(ape)
dis_matrix <- arrow::read_parquet('dis_matrix_full.parquet')
dis_matrix <- as.matrix(dis_matrix)
rownames(dis_matrix) <- colnames(dis_matrix)
weight_matrix <- 1/dis_matrix^2
rm(dis_matrix)

Moran.I(nvi$tisza, weight_matrix)
nrow(nvi)
nrow(weight_matrix)

setdiff(colnames(weight_matrix), nvi$name)

cnames <- colnames(weight_matrix)
cnames <- cnames[order(cnames)]
nnames <- nvi$name
nnames <- nnames[order(nnames)]

cnames <- cnames[!cnames %in% c('Budapest')]
