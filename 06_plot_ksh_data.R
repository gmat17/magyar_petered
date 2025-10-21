setwd("~/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data")
Sys.setlocale("LC_CTYPE", "UTF-8")

# ---- Read neceassary files -----
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
get_combined('animal_unity', islog=FALSE)
get_combined('animal_unity', islog=TRUE) # using log1p

# big_flats
get_combined('big_flats')

# waste_collection
get_combined('waste_collection')
get_combined('waste_collection',islog=TRUE)

# flat_sewage
get_combined('flat_sewage')

# criminals
get_combined('criminals')
get_combined('criminals',islog=TRUE)

# szja
get_combined('szja')

# flat_area
get_combined('flat_area')

# sewage_quantity
get_combined('sewage_quantity')
get_combined('sewage_quantity',islog=TRUE)

# prof_per_stud
get_combined('prof_per_stud')
get_combined('prof_per_stud',islog=TRUE)

# collected_waste
get_combined('collected_waste')
get_combined('collected_waste',islog=TRUE)

# gas_consumption 
get_combined('gas_consumption')

# electricity
get_combined('electricity_consumption')
get_combined('electricity_consumption',islog=TRUE)
# put to quintiles --> bins == categorical

# cultural_programs
get_combined('cultural_programs')
get_combined('cultural_programs',islog=TRUE)

# newborns
get_combined('newborns')
get_combined('newborns',islog=TRUE)

# businesses
get_combined('businesses')
get_combined('businesses',islog=TRUE)

# deaths
get_combined('deaths')
get_combined('deaths',islog=TRUE)

# marriages
get_combined('marriages')
get_combined('marriages',islog=TRUE)

# net_subs
get_combined('net_subs')
get_combined('net_subs',islog=TRUE)

# small_stores
get_combined('small_stores')
get_combined('small_stores',islog=TRUE)

# estate_area
get_combined('estate_area')
get_combined('estate_area',islog=TRUE)

# pensioneers
get_combined('pensioneers')

# migration_diff
get_combined('migration_diff')

# crop_field
get_combined('crop_field')
get_combined('crop_field',islog=TRUE)

# crop_field_per_pop
get_combined('crop_field_per_pop')
get_combined('crop_field_per_pop',islog=TRUE)

# habitans_per_flats
get_combined('habitans_per_flats')
get_combined('habitans_per_flats',islog=TRUE)

# childs_per_nursery_school
get_combined('childs_per_nursery_school')
get_blank_hist('childs_per_nursery_school')
# 3 cat var --> 0, below median, above median

# cars
get_combined('cars')
get_combined('cars',islog=TRUE)
# correlation with szja

# fertility_rate
get_combined('fertility_rate')

# len_routes_diff --> ratio? --> ha jobb, akkor megtartom, ha rosszabb, akkor drop it
get_combined('len_routes_diff')

# flats
get_combined('flats')

# building_permissions
get_combined('building_permissions')

# ---- Nepszamlalas data ----
# age
ages <- c('age10','age20','age30','age40','age50','age60',
          'age70','age80','age90')
get_boxplot(ages)

# edu
edus <- c('lower_elementary', 'elementary', 'degree', 'leaving_exam', 'uni')
get_boxplot(edus)

# population
get_combined('pop',islog=FALSE)
get_combined('pop',islog=TRUE)

# ---- Correlation plots ----
library(corrplot)

# without logs
cor.mat <- cor(as.data.frame(ksh[,7:36])[1:30])
corrplot(cor.mat, method='square')

# read-in descriptions once again
desc <- readxl::read_excel('ksh_data_concated.xlsx', sheet = 'description')
desc$description <- stri_trans_nfc(desc$description)
csvin <- desc[grepl('.csv',desc$description),]
csvin$description <- substr(csvin$description, 1, nchar(csvin$description) - 4)
desc[csvin$name==desc$name,]$description <- csvin$description

stat <- ksh
for (i in desc$name){
  if (desc[desc$name==i,'using_log1p']==1) {stat[[i]] <- log1p(ksh[[i]])}
}
stat <- stat[,c(1:28,54,30:53)]

# with logs
cor.mat <- cor(as.data.frame(stat[,c(7:36,52)])[1:31])
corrplot(cor.mat, method='square')

# only medium and strong correlation
cor.mat[abs(cor.mat)<0.3] <- 0
corrplot(cor.mat, method='square')
(sum(abs(cor.mat)>0)-nrow(cor.mat))/2 # 58
# easy to interpret the similar values
  # upper-left corner: 
    # flat_sewage, szja, flat_area, sewage_quantity, collected_waste, gas_consumption
  # lower part:
    # habitans_per_flats, childs_per_nursery_school, cars
  # pairs:
    # businesses-criminals --> WTF
    # 
    


# only medium-strong correlation
cor.mat[abs(cor.mat)<0.5] <- 0
corrplot(cor.mat, method='square')
(sum(abs(cor.mat)>0)-nrow(cor.mat))/2 # 11

# only strongly correlation
cor.mat[abs(cor.mat)<0.7] <- 0
corrplot(cor.mat, method='square')
(sum(abs(cor.mat)>0)-nrow(cor.mat))/2 # 4

# ---- Scatterplots ----


