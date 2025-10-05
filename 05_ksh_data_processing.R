# ---- Disclaimer ----
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

# ---- Loading TEIR table ----

setwd("~/Downloads/egyetem/TDK/magyar_petered_main/ksh_data")
# path <- '~/Downloads/egyetem/TDK/magyar_petered_main/ksh_data/'

goods <- list.files('1_jó')
teir <- readxl::read_excel('TEIR_TÁBLÁZAT 2025922_14-23-0.xlsx', sheet='lap_0')
teir <- teir[!is.na(teir$kod),]
check_teir <- function(x){if (grepl(" \\*$", x)){substr(x, 1, nchar(x) - 2)}else{x}}
teir$...1 <- sapply(teir$...1,FUN = check_teir)

# --- Load base table ----
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
# Age
age <- readxl::read_excel('ksh-census2022-korcsopok.xlsx')
base_table <- merge(base_table, age, by.x='name', by.y='...1')

# Education
edu <- readxl::read_excel('ksh-census2022-iskola.xlsx')
base_table <- merge(base_table, edu, by.x='name', by.y='...1')

# ---- Exporting the final table ----
writexl::write_xlsx(base_table, 'ksh_data_concated.xlsx')

