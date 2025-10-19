setwd("~/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data")
Sys.setlocale("LC_CTYPE", "UTF-8")

# ---- Read neceassary files -----
library(sf)
library(leaflet)
library(stringi)

# load ksh data
ksh <- readxl::read_excel('ksh_data_concated.xlsx')
ksh$is_mped <- as.logical(ksh$is_mped)
summary(ksh)

# load munis' shapefile
shape_df <- st_read('kozighatarok/admin8.shp')
shape_df <- st_make_valid(shape_df)
shape_df <- st_transform(shape_df, crs = 4326)
ksh <- merge(ksh, shape_df[,c('NAME', 'geometry')], by.x='name', by.y='NAME')
ksh <- st_as_sf(ksh)

# load hun shapefile
# load hungary's shape
hun_shape <- st_read('kozighatarok/admin2.shp')
hun_shape <- st_make_valid(hun_shape)
hun_shape <- st_transform(hun_shape, crs = 4326)

# descriptions
desc <- readxl::read_excel('ksh_data_concated.xlsx', sheet = 'description')
desc$description <- stri_trans_nfc(desc$description)
csvin <- desc[grepl('.csv',desc$description),]
csvin$description <- substr(csvin$description, 1, nchar(csvin$description) - 4)
desc[csvin$name==desc$name,]$description <- csvin$description

# ---- Preparation of plotting ----
library(ggplot2)
library(RColorBrewer)
library(tidyr)

bins <- c(0, quantile(ksh$animal_unity, 
                          probs = c(0.2,0.4,0.6,0.8)), Inf)
palette <- colorBin('YlOrRd', domain = ksh$animal_unity, 
                 bins = bins)

# ---- is_mped ----
ggplot(ksh) +
  geom_sf(data=hun_shape, fill='white', size=0.3,color='black') +
  geom_sf(aes(fill = is_mped), color = 'white', size=0.01) +
  scale_fill_manual(name = '', values = c('TRUE' = "lightgreen", 'FALSE' = "brown1"),
                    labels = c('Hamis', 'Igaz')) +
  theme_minimal() +
  theme(legend.position='bottom') +
  labs(title = 'Települések, ahol Magyar Péter országjárást tartott', ) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
        axis.title = element_blank(), axis.text=element_blank(), panel.grid=element_blank())

# ---- Boxplots ----
# about all numeric variables
per_cols <- c('big_flats', 'flat_sewage', 'pensioneers')
not_out_col <- c('flat_area', 'prof_per_stud', 'deaths', 'marriages', 'migration_diff', 
                 'newborns', 'small_stores', 'building_permissions',
                 'childs_per_nursery_school', 'fertility_rate', 'flats', 'habitans_per_flats')
some_out_col <- c('collected_waste', 'gas_consumption', 'sewage_quantity', 'waste_collection'
                  , 'estate_area', 'net_subs', 'len_routes_diff')
great_out_col <- c('animal_unity','businesses','szja')
miaf <- c('criminals','cultural_programs', 'electricity_consumption', 'cars', 'crop_field')

# function for the plot
get_boxplot <- function(x){
  # unpivot the table
  ksh_filt <- ksh[,x]
  ksh_unpiv <- ksh_filt %>%
    pivot_longer(
      cols = 1:(ncol(ksh_filt) - 1),
      names_to = "variable",
      values_to = "value"
    )
  ksh_unpiv <- ksh_unpiv[,c('variable','value')]
  
  # generate the ggplot
  ggplot(ksh_unpiv, aes(x = variable, y = value)) +
    geom_boxplot() +
    stat_summary(fun = mean, geom = "crossbar", , color = "red", width = 0.75, size=0.25) +
    theme_minimal() +
    labs(title = 'Egyes változók eloszlása és átlaga', 
         subtitle = 'Átlag: piros vonal, Medián: fekete vonal') +
    theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
          plot.subtitle = element_text(hjust = 0.5, face = 'italic', size=10),
          axis.text.x = element_text(angle = 45, hjust = 1))
}

# call the function
get_boxplot(per_cols)
get_boxplot(not_out_col)
get_boxplot(some_out_col)
get_boxplot(great_out_col)
get_boxplot(miaf)

# ---- Functions for the plots ----
library(dplyr)
library(grid)

get_mapplot <- function(x,islog=FALSE){
  title_text <- desc %>% 
    filter(name == x) %>% 
    pull(description)
  
  ggplot(ksh) +
    geom_sf(data=hun_shape, fill='white', size=0.3,color='black') +
    geom_sf(aes(fill = if (islog) log(.data[[x]]) else .data[[x]]), 
            color = 'white', size = 0.01) +
    scale_fill_viridis_b(option = 'viridis') +
    theme_minimal() +
    theme(legend.position='bottom') +
    labs(title = desc[desc$name==x,]$description,
         fill = if (islog) paste0('log(',x,')') else x) +
    theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
          axis.title = element_blank(), axis.text=element_blank(), panel.grid=element_blank()) +
    guides(fill = guide_colorbar(barwidth = 10, barheight = 0.5))
}

get_hist <- function(x,islog=FALSE){
  if (islog) {for_fun <- log(ksh[[x]])} else {for_fun <- ksh[[x]]}
  
  for_fun[(for_fun==-Inf)|(for_fun==Inf)] <- 0
  
  g <- ggplot(ksh) +
    geom_histogram(aes(x= for_fun)) +
    scale_fill_viridis_b(option = 'viridis') +
    stat_function(fun = dnorm, 
                  args = list(mean = mean(for_fun,na.rm=TRUE), 
                              sd = sd(for_fun)), 
                  color = 'red') +
    theme_minimal() +
    theme(legend.position='bottom', panel.background = element_blank(),
          plot.background = element_blank(), panel.grid = element_blank()) +
    labs(y='darab', x = if (islog) paste0('log(',x,')') else x) +
    theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14))
  return(g)
}

ggplot(data.frame(x = c(-2, 8)), aes(x = x)) +
  stat_function(fun = dnorm, args = list(mean = 2.41364, sd = 1.339745))

get_combined <- 

# animal unity
p1 <- get_mapplot('animal_unity',islog=FALSE)
get_mapplot('animal_unity',islog=TRUE)

p2 <- get_hist('animal_unity', islog=FALSE)
get_hist('animal_unity', islog=TRUE)

g1 <- ggplotGrob(p1)
g2 <- ggplotGrob(p2)

grid.newpage()
grid.draw(g1)
pushViewport(viewport())
grid.draw(editGrob(g2, gp = gpar(alpha = 0.3)))
popViewport()

# animal_unity


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

# ---- Nepszamlalas data ----
# age
ages <- c('age10','age20','age30','age40','age50','age60',
          'age70','age80','age90')
get_boxplot(ages)

# edu
edus <- c('lower_elementary', 'elementary', 'degree', 'leaving_exam', 'uni')
get_boxplot(edus)

# population



