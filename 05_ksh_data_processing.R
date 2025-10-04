setwd("~/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered")
path <- '~/Downloads/egyetem/TDK/magyar_petered_main/ksh_data/'

goods <- list.files(path = paste0(path, '1_jó'))
teir <- readxl::read_excel(paste0(path, 'TEIR_TÁBLÁZAT 2025922_14-23-0.xlsx'))
teir <- teir[!is.na(teir$kod),]
check_teir <- function(x){if (grepl(" \\*$", x)){substr(x, 1, nchar(x) - 2)}else{x}}
teir$...1 <- sapply(teir$...1,FUN = check_teir)

# --- Load base table ----
file <- '~/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data/municipality_lonlat.csv'
base_table <- read.csv(file)
length(base_table$name)
base_table <- base_table[!duplicated(base_table$name),]

# ---- Load 28 files from 1_jo ----
rename_col <- function(df, old_name, new_name){
  names(df)[names(df) == old_name] <- new_name
  return (df)
}

# add code to base table
code_df <- read.csv(paste0(path, '1_jó/', goods[1]), sep=';')[,c('ELEM_KOD','TELEP_NEV')]
base_table <- merge(code_df, base_table, by.x='TELEP_NEV','name')
names(base_table) <- c('name', 'id', "place", "is_mped", "x", "y")

for (i in 1:length(goods)){
  mini_df <- read.csv(paste0(path, '1_jó/', goods[i]), sep=';')[,c('ELEM_KOD','VALUE')]
  mini_df <- rename_col(mini_df, 'VALUE', goods[i])
  base_table <- merge(base_table, mini_df, by.x='id', by.y = 'ELEM_KOD', all.x = TRUE)
}

# ---- Join 28 files with TEIR ----
length(teir$...1)
length(base_table$name)

base_table <- merge(base_table, teir, by.x='id', by.y='kod', all.x=TRUE)
writexl::write_xlsx(base_table, paste0(path, 'munis_and_ksh_data_1andteir.xlsx'))
# ---- Load XX files from 0_problemas ----



