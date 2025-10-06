setwd("~/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data")

# ---- Read neceassary files -----
library(sf)
library(leaflet)

ksh <- readxl::read_excel('ksh_data_concated_full.xlsx')
ksh$is_mped <- as.logical(ksh$is_mped)
summary(ksh)
shape_df <- st_read('kozighatarok/admin8.shp')
shape_df <- st_make_valid(shape_df)
shape_df <- st_transform(shape_df, crs = 4326)
ksh <- merge(ksh, shape_df[,c('NAME', 'geometry')], by.x='name', by.y='NAME')
ksh <- st_as_sf(ksh)

# ahol NA oda put 0

# ---- Plotting ----
library(ggplot2)
library(RColorBrewer)

bins <- c(0, quantile(ksh$animal_unity, 
                          probs = c(0.2,0.4,0.6,0.8)), Inf)
palette <- colorBin('YlOrRd', domain = ksh$animal_unity, 
                 bins = bins)
# is_mped
ggplot(ksh) +
  geom_sf(aes(fill = is_mped), color = 'white', size=0.01) +
  scale_fill_manual(values = c('TRUE' = "lightgreen", 'FALSE' = "brown1")) +
  theme_minimal() +
  theme(legend.position='bottom')

# animal_unity
ggplot(ksh) +
  geom_sf(aes(fill = log(animal_unity)), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal() +
  theme(legend.position='bottom')
# convert to log

# Big flats
ggplot(ksh) +
  geom_sf(aes(fill = big_flats), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()

# 
ggplot(ksh) +
  geom_sf(aes(fill = big_flats), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()

ggplot(ksh) +
  geom_sf(aes(fill = log(waste_collection)), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()

ggplot(ksh) +
  geom_sf(aes(fill = flat_sewage), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()

# criminals
ggplot(ksh) +
  geom_sf(aes(fill = log(criminals)), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()

# szja --> nem log!!!
ggplot(ksh) +
  geom_sf(aes(fill = szja), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()
hist(ksh$szja)

# flat_area
ggplot(ksh) +
  geom_sf(aes(fill = flat_area), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()

ggplot(ksh) +
  geom_sf(aes(fill = log(sewage_quantity)), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()
hist(log(ksh$sewage_quantity))

# prof_per_stud
ggplot(ksh) +
  geom_sf(aes(fill = log(prof_per_stud)), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()
hist(log(ksh$prof_per_stud))

# 
ggplot(ksh) +
  geom_sf(aes(fill =  log(collected_waste)), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()
hist(ksh$collected_waste)

ggplot(ksh) +
  geom_sf(aes(fill =  gas_consumption), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()
hist(ksh$gas_consumption)

# electricity
ggplot(ksh) +
  geom_sf(aes(fill =  log(electricity_consumption)), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()
# put to quintiles --> bins == categorical

ggplot(ksh) +
  geom_sf(aes(fill =  log(cultural_programs)), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()

ggplot(ksh) +
  geom_sf(aes(fill =  newborns), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()

ggplot(ksh) +
  geom_sf(aes(fill =  log(businesses)), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()
hist(log(ksh$businesses))

ggplot(ksh) +
  geom_sf(aes(fill =  log(deaths)), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()

ggplot(ksh) +
  geom_sf(aes(fill =  log(marriages)), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()
hist(log(ksh$marriages))

ggplot(ksh) +
  geom_sf(aes(fill =  log(cars)), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()
# correlation with szja

ggplot(ksh) +
  geom_sf(aes(fill =  building_permissions), color = 'white', size=0.01) +
  scale_fill_viridis_b(option = 'viridis') +
  theme_minimal()
hist(log(ksh$building_permissions))



