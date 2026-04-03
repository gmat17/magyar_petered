setwd("~/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data")
Sys.setlocale("LC_CTYPE", "UTF-8")

# ---- 1. Read neceassary files -----
library(sf)
library(leaflet)
library(stringi)
library(patchwork)

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

# load hun shapefile
# load hungary's shape
hun_shape <- st_read('kozighatarok/admin2.shp')
hun_shape <- st_make_valid(hun_shape)
hun_shape <- st_transform(hun_shape, crs = 4326)

# descriptions
desc <- readxl::read_excel('description.xlsx', sheet = 'description')
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
out_col <- c('criminals','cultural_programs', 'electricity_consumption', 'cars', 'crop_field')

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
    #labs(title = 'Egyes változók eloszlása és átlaga', 
    #     subtitle = 'Átlag: piros vonal, Medián: fekete vonal') +
    theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
          plot.subtitle = element_text(hjust = 0.5, face = 'italic', size=10),
          axis.text.x = element_text(angle = 45, hjust = 1))
}

plot.path <- '/Users/mac/Downloads/egyetem/TDK/magyar_petered_main/plots/'

# call the function
b1 <- get_boxplot(per_cols)
b2 <- get_boxplot(not_out_col)
b3 <- get_boxplot(some_out_col)
b4 <- get_boxplot(great_out_col)
b5 <- get_boxplot(out_col)
b6 <- get_boxplot('doctor')

wrap_plots(b1, b2, b3, b4, b5, b6, ncol = 3) + 
  plot_annotation(
    title = 'Egyes változók eloszlása és átlaga',
    subtitle = 'Átlag: piros vonal, Medián: fekete vonal',
    theme = theme(
      plot.title = element_text(size = 22, face = "bold"),
      plot.subtitle = element_text(size = 14)
    )
  ) & 
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1, size = 13),
    axis.title.x = element_blank(),
    axis.title.y = element_blank()
  )

ggsave(filename=paste0(plot.path, 'v02_02_boxplots.png'), 
width = 4000, height = 4000, units = "px", dpi = 300)

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

get_mapplot <- function(x,islog=FALSE,istitle=T){ 
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
    labs(title = if (istitle) desc[desc$name==x,]$description else '',
         fill = if (islog) paste0('log1p(',x,')') else x) +
    theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
          axis.title = element_blank(), axis.text=element_blank(), panel.grid=element_blank()) +
    guides(fill = guide_colorbar(barwidth = 10, barheight = 0.5))
}

get_hist <- function(x,islog=FALSE,istitle=T){
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
    labs(y='darab', x = if (islog) paste0('log1p(',x,')') else x) +
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

plot.path.ksh <- '/Users/mac/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/ksh_plots/'

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
get_hist_no_title <- function(x,islog=FALSE){
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
    labs(y='darab', x = if (islog) paste0('log1p(',x,')') else x) +
    theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 14))
  return(g)
}

h1 <- get_hist('animal_unity', islog=FALSE)
h2 <- get_mapplot('animal_unity', islog=F,istitle=F)
h3 <- get_hist('animal_unity', islog=T)
h4 <- get_mapplot('animal_unity', islog=T,istitle=F)

(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path, 'v02_03_animal_unity.png'), 
width = 3000, height = 2000, units = "px", dpi = 300)

# big_flats
h1 <- get_hist('big_flats')
h2 <- get_mapplot('big_flats')
h1|h2
ggsave(filename=paste0(plot.path.ksh, 'big_flats.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# waste_collection
h1 <- get_hist('waste_collection')
h2 <- get_mapplot('waste_collection')
h3 <- get_hist('waste_collection',islog=T)
h4 <- get_mapplot('waste_collection',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'waste_collection.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# flat_sewage
h1 <- get_hist('flat_sewage')
h2 <- get_mapplot('flat_sewage')
h1|h2
ggsave(filename=paste0(plot.path.ksh, 'flat_sewage.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# criminals
h1 <- get_hist('criminals')
h2 <- get_mapplot('criminals')
h3 <- get_hist('criminals',islog=T)
h4 <- get_mapplot('criminals',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'criminals.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# doctor
h1 <- get_hist('doctor')
h2 <- get_mapplot('doctor')
h3 <- get_hist('doctor',islog=T)
h4 <- get_mapplot('doctor',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'doctor.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# szja
h1 <- get_hist('szja')
h2 <- get_mapplot('szja')
h1|h2
ggsave(filename=paste0(plot.path.ksh, 'szja.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# flat_area
h1 <- get_hist('flat_area')
h2 <- get_mapplot('flat_area')
h1|h2
ggsave(filename=paste0(plot.path.ksh, 'flat_area.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# sewage_quantity
h1 <- get_hist('sewage_quantity')
h2 <- get_mapplot('sewage_quantity')
h3 <- get_hist('sewage_quantity',islog=T)
h4 <- get_mapplot('sewage_quantity',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'sewage_quantity.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# prof_per_stud
h1 <- get_hist('prof_per_stud')
h2 <- get_mapplot('prof_per_stud')
h3 <- get_hist('prof_per_stud',islog=T)
h4 <- get_mapplot('prof_per_stud',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'prof_per_stud.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# collected_waste
h1 <- get_hist('collected_waste')
h2 <- get_mapplot('collected_waste')
h3 <- get_hist('collected_waste',islog=T)
h4 <- get_mapplot('collected_waste',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'collected_waste.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# gas_consumption
h1 <- get_hist('gas_consumption')
h2 <- get_mapplot('gas_consumption')
h1|h2
ggsave(filename=paste0(plot.path.ksh, 'gas_consumption.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# electricity
h1 <- get_hist('electricity_consumption')
h2 <- get_mapplot('electricity_consumption')
h3 <- get_hist('electricity_consumption',islog=T)
h4 <- get_mapplot('electricity_consumption',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'electricity_consumption.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# cultural_programs
h1 <- get_hist('cultural_programs')
h2 <- get_mapplot('cultural_programs')
h3 <- get_hist('cultural_programs',islog=T)
h4 <- get_mapplot('cultural_programs',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'cultural_programs.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# newborns
h1 <- get_hist('newborns')
h2 <- get_mapplot('newborns')
h3 <- get_hist('newborns',islog=T)
h4 <- get_mapplot('newborns',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'newborns.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# businesses
h1 <- get_hist('businesses')
h2 <- get_mapplot('businesses')
h3 <- get_hist('businesses',islog=T)
h4 <- get_mapplot('businesses',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'businesses.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# deaths
h1 <- get_hist('deaths')
h2 <- get_mapplot('deaths')
h3 <- get_hist('deaths',islog=T)
h4 <- get_mapplot('deaths',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'deaths.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# marriages
h1 <- get_hist('marriages')
h2 <- get_mapplot('marriages')
h3 <- get_hist('marriages',islog=T)
h4 <- get_mapplot('marriages',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'marriages.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# net_subs
h1 <- get_hist('net_subs')
h2 <- get_mapplot('net_subs')
h3 <- get_hist('net_subs',islog=T)
h4 <- get_mapplot('net_subs',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'net_subs.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# small_stores
h1 <- get_hist('small_stores')
h2 <- get_mapplot('small_stores')
h3 <- get_hist('small_stores',islog=T)
h4 <- get_mapplot('small_stores',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'small_stores.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# estate_area
h1 <- get_hist('estate_area')
h2 <- get_mapplot('estate_area')
h3 <- get_hist('estate_area',islog=T)
h4 <- get_mapplot('estate_area',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'estate_area.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# pensioneers
h1 <- get_hist('pensioneers')
h2 <- get_mapplot('pensioneers')
h1|h2
ggsave(filename=paste0(plot.path.ksh, 'pensioneers.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# migration_diff
h1 <- get_hist('migration_diff')
h2 <- get_mapplot('migration_diff')
h1|h2
ggsave(filename=paste0(plot.path.ksh, 'migration_diff.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# crop_field
h1 <- get_hist('crop_field')
h2 <- get_mapplot('crop_field')
h3 <- get_hist('crop_field',islog=T)
h4 <- get_mapplot('crop_field',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'crop_field.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# habitans_per_flats
h1 <- get_hist('habitans_per_flats')
h2 <- get_mapplot('habitans_per_flats')
h3 <- get_hist('habitans_per_flats',islog=T)
h4 <- get_mapplot('habitans_per_flats',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'habitans_per_flats.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# childs_per_nursery_school
h1 <- get_hist('habitans_per_flats')
h2 <- get_mapplot('habitans_per_flats')
h1|h2
ggsave(filename=paste0(plot.path.ksh, 'childs_per_nursery_school.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# cars
h1 <- get_hist('cars')
h2 <- get_mapplot('cars')
h3 <- get_hist('cars',islog=T)
h4 <- get_mapplot('cars',islog=T)
(h1|h3)/(h2|h4)
ggsave(filename=paste0(plot.path.ksh, 'cars.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# fertility_rate
h1 <- get_hist('fertility_rate')
h2 <- get_mapplot('fertility_rate')
h1|h2
ggsave(filename=paste0(plot.path.ksh, 'fertility_rate.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# len_routes_diff
h1 <- get_hist('len_routes_diff')
h2 <- get_mapplot('len_routes_diff')
h1|h2
ggsave(filename=paste0(plot.path.ksh, 'len_routes_diff.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# flats
h1 <- get_hist('flats')
h2 <- get_mapplot('flats')
h1|h2
ggsave(filename=paste0(plot.path.ksh, 'flats.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# building_permissions
h1 <- get_hist('building_permissions')
h2 <- get_mapplot('building_permissions')
h1|h2
ggsave(filename=paste0(plot.path.ksh, 'building_permissions.png'), 
     width = 3500, height = 2000, units = "px", dpi = 300)

# ---- Nepszamlalas data ----
# age
ages <- c('age10','age20','age30','age40','age50','age60',
          'age70','age80','age90')
n1 <- get_boxplot(ages)

# edu
edus <- c('lower_elementary', 'elementary', 'degree', 'leaving_exam', 'uni')
n2 <- get_boxplot(edus)

# religion
rel <- c('christian', 'ateist', 'rel_no_ans')
n3 <- get_boxplot(rel)

# nationality
nat <- c('hun', 'roma', 'other_nat', 'roma_no_ans')
n4 <- get_boxplot(nat)

wrap_plots(n1, n2, n3, n4, ncol = 2) + 
  plot_annotation(
    title = 'Egyes változók eloszlása és átlaga',
    subtitle = 'Átlag: piros vonal, Medián: fekete vonal',
    theme = theme(
      plot.title = element_text(size = 22, face = "bold"),
      plot.subtitle = element_text(size = 14)
    )
  ) & 
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1, size = 13),
    axis.title.x = element_blank(),
    axis.title.y = element_blank()
  )
ggsave(filename=paste0(plot.path, 'v02_04_nepszamlalas.png'), 
width = 3000, height = 2000, units = "px", dpi = 300)

# population
p1 <- get_hist('pop')
p2 <- get_mapplot('pop',istitle=F)
p3 <- get_hist('pop',islog=T)
p4 <- get_mapplot('pop',islog=T,istitle=F)

(p1|p3)/(p2|p4)
ggsave(filename=paste0(plot.path, 'v02_05_pop.png'), 
width = 3000, height = 2000, units = "px", dpi = 300)

# summary table
colnames(ksh)
summary(ksh[10:63])
