setwd("~/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data")
Sys.setlocale("LC_CTYPE", "UTF-8")

# ---- 1. Read neceassary files -----
library(sf)
library(leaflet)
library(stringi)

# load ksh data
ksh <- readxl::read_excel('ksh_data_concated.xlsx')
ksh <- ksh %>% replace(is.na(.), 0)
ksh$is_mped <- as.logical(ksh$is_mped)
summary(ksh)

# load munis' shapefile
shape_df <- st_read('kozighatarok/admin8.shp')
shape_df <- st_make_valid(shape_df)
shape_df <- st_transform(shape_df, crs = 4326)
ksh <- merge(ksh, shape_df[,c('NAME', 'geometry')], by.x='name', by.y='NAME')
ksh <- st_as_sf(ksh)
ksh$crop_field_per_pop <- (ksh$crop_field*1e6)/st_area(ksh$geometry)

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

# ---- Boxplots ----
library(ggplot2)
library(RColorBrewer)
library(tidyr)
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

# Quick guide for the functions, since there are some:
  # get_mapplot: map of munis, colouring based on the value of the muni
    # params: x ~ variable name AND islog=FALSE ~ use log1p function or not, default not

  # get_hist: get a nice histogram of the given varaible
    # params: x ~ variable name AND islog=FALSE ~ use log1p function or not, default not

  # get_combined: combine functions above, but the histogram is in the background, it is less
  # decorated than previously. the map has no change
    # params: x ~ variable name AND islog=FALSE ~ use log1p function or not, default not
    # params are for BOTH, not possible to combine variables

get_mapplot <- function(x,islog=FALSE){
  title_text <- desc %>% 
    filter(name == x) %>% 
    pull(description)
  
  ggplot(ksh) +
    geom_sf(data=hun_shape, fill='white', size=0.3,color='black') +
    geom_sf(aes(fill = if (islog) log1p(.data[[x]]) else .data[[x]]), 
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
  if (islog) {for_fun <- log1p(ksh[[x]])} else {for_fun <- ksh[[x]]}
  
  g <- ggplot(ksh) +
    geom_histogram(aes(x= for_fun)) +
    scale_fill_viridis_b(option = 'viridis') +
    stat_function(fun = function(x) {
      dnorm(x, mean = mean(for_fun, na.rm = TRUE), sd = sd(for_fun, na.rm = TRUE)) * 
        diff(hist(for_fun, plot = FALSE)$mids[1:2]) * length(for_fun)
    }, color = "red", size = 1, alpha = 0.5)  +
    theme_minimal() +
    theme(legend.position='bottom', panel.background = element_blank(),
          plot.background = element_blank(), panel.grid = element_blank()) +
    labs(y='darab', x = if (islog) paste0('log(',x,')') else x) +
    theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14))
  return(g)
}

get_blank_hist <- function(x,islog=FALSE){
  if (islog) {for_fun <- log1p(ksh[[x]])} else {for_fun <- ksh[[x]]}
  g <- ggplot(ksh, aes(x = for_fun)) +
    geom_histogram() +
    stat_function(fun = function(x) {
      dnorm(x, mean = mean(for_fun, na.rm = TRUE), sd = sd(for_fun, na.rm = TRUE)) * 
        diff(hist(for_fun, plot = FALSE)$mids[1:2]) * length(for_fun)
    }, color = "red", size = 1, alpha = 0.5) +
    theme(legend.position='bottom', panel.background = element_blank(),
          plot.background = element_blank(), panel.grid = element_blank(), 
          axis.title = element_blank())
  return(g)
}

get_combined <- function(x,islog=FALSE){
  p1 <- get_mapplot(x,islog=islog)
  p2 <- get_blank_hist(x, islog=islog)
  g1 <- ggplotGrob(p1)
  g2 <- ggplotGrob(p2)
  
  grid.newpage()
  grid.draw(g1)
  pushViewport(viewport())
  grid.draw(editGrob(g2, gp = gpar(alpha = 0.3)))
  popViewport()
}

# ---- Plotting individual values ----
library(units)

bins <- c(0, quantile(ksh$animal_unity, 
                      probs = c(0.2,0.4,0.6,0.8)), Inf)
palette <- colorBin('YlOrRd', domain = ksh$animal_unity, 
                    bins = bins)

# is_mped --> not used the above function, since it a binomial value
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

# animal unity
get_hist('animal_unity', islog=FALSE)
get_mapplot('animal_unity', islog=F)
get_hist('animal_unity', islog=T)
get_mapplot('animal_unity', islog=T)

# big_flats
get_hist('big_flats')
get_mapplot('big_flats')

# waste_collection
get_hist('waste_collection')
get_mapplot('waste_collection')
get_hist('waste_collection',islog=T)
get_mapplot('waste_collection',islog=T)

# flat_sewage
get_hist('flat_sewage')
get_mapplot('flat_sewage')

# criminals
get_hist('criminals')
get_mapplot('criminals')
get_hist('criminals',islog=T)
get_mapplot('criminals',islog=T)

# szja
get_hist('szja')
get_mapplot('szja')

# flat_area
get_hist('flat_area')
get_mapplot('flat_area')

# sewage_quantity
get_hist('sewage_quantity')
get_mapplot('sewage_quantity')
get_hist('sewage_quantity',islog=T)
get_mapplot('sewage_quantity',islog=T)

# prof_per_stud
get_hist('prof_per_stud')
get_mapplot('prof_per_stud')
get_hist('prof_per_stud',islog=T)
get_mapplot('prof_per_stud',islog=T)

# collected_waste
get_hist('collected_waste')
get_mapplot('collected_waste')
get_hist('collected_waste',islog=T)
get_mapplot('collected_waste',islog=T)

# gas_consumption
get_hist('gas_consumption')
get_mapplot('gas_consumption')

# electricity
get_hist('electricity_consumption')
get_mapplot('electricity_consumption')
get_hist('electricity_consumption',islog=T)
get_mapplot('electricity_consumption',islog=T)

# cultural_programs
get_hist('cultural_programs')
get_mapplot('cultural_programs')
get_hist('cultural_programs',islog=T)
get_mapplot('cultural_programs',islog=T)

# newborns
get_hist('newborns')
get_mapplot('newborns')
get_hist('newborns',islog=T)
get_mapplot('newborns',islog=T)

# businesses
get_hist('businesses')
get_mapplot('businesses')
get_hist('businesses',islog=T)
get_mapplot('businesses',islog=T)

# deaths
get_hist('deaths')
get_mapplot('deaths')
get_hist('deaths',islog=T)
get_mapplot('deaths',islog=T)

# marriages
get_hist('marriages')
get_mapplot('marriages')
get_hist('marriages',islog=T)
get_mapplot('marriages',islog=T)

# net_subs
get_hist('net_subs')
get_mapplot('net_subs')
get_hist('net_subs',islog=T)
get_mapplot('net_subs',islog=T)

# small_stores
get_hist('small_stores')
get_mapplot('small_stores')
get_hist('small_stores',islog=T)
get_mapplot('small_stores',islog=T)

# estate_area
get_hist('estate_area')
get_mapplot('estate_area')
get_hist('estate_area',islog=T)
get_mapplot('estate_area',islog=T)

# pensioneers
get_hist('pensioneers')
get_mapplot('pensioneers')

# migration_diff
get_hist('migration_diff')
get_mapplot('migration_diff')

# crop_field
get_hist('crop_field')
get_mapplot('crop_field')
get_hist('crop_field',islog=T)
get_mapplot('crop_field',islog=T)

# habitans_per_flats
get_hist('habitans_per_flats')
get_mapplot('habitans_per_flats')
get_hist('habitans_per_flats',islog=T)
get_mapplot('habitans_per_flats',islog=T)

# childs_per_nursery_school
get_hist('habitans_per_flats')
get_mapplot('habitans_per_flats')

# cars
get_hist('cars')
get_mapplot('cars')
get_hist('cars',islog=T)
get_mapplot('cars',islog=T)
# correlation with szja

# fertility_rate
get_hist('fertility_rate')
get_mapplot('fertility_rate')

# len_routes_diff
get_hist('len_routes_diff')
get_mapplot('len_routes_diff')

# flats
get_hist('flats')
get_mapplot('flats')

# building_permissions
get_hist('building_permissions')
get_mapplot('building_permissions')

# ---- Nepszamlalas data ----
# age
ages <- c('age10','age20','age30','age40','age50','age60',
          'age70','age80','age90')
get_boxplot(ages)

# edu
edus <- c('lower_elementary', 'elementary', 'degree', 'leaving_exam', 'uni')
get_boxplot(edus)

# population
get_hist('pop')
get_mapplot('pop')
get_hist('pop',islog=T)
get_mapplot('pop',islog=T)

