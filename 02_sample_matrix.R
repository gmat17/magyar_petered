# ---- Read-in file and create coordinate column ----
df=read.csv('data/municipality_lonlat.csv')
df$coord <- as.matrix(df[,c('x', 'y')])
rownames(df) <- 1:nrow(df)

head(df)
head(df[1, 'coord'])

# ---- Take a sample from the whole dataframe ----
set.seed(2*17)
df_sample <- df[sample(rownames(df), size=15, replace=FALSE),]
rownames(df_sample) <- df_sample$name

df_sample[4, 'coord']

# ---- Select the best distance calculating method ----
# --> distHaversine <-- fastest
library(geosphere)
system.time(
distHaversine(
  df_sample[1, 'coord'],
  df_sample[2, 'coord']
  )/1000 # result in meters --> put to km
)

# ---- Create matrix ----
mp <- matrix(nrow=nrow(df_sample), ncol=nrow(df_sample))
rownames(mp) <- df_sample$name
colnames(mp) <- df_sample$name
mp[is.na(mp)] <- 0
mp

rownames(mp)[2]

# ---- Create in_100km matrix ----
# --> not true or false since low amount of data, fast to calculate 
# --> later: possibility to find inflexion point

in_100m <- matrix(nrow=nrow(df_sample), ncol=nrow(df_sample))
rownames(in_100m) <- df_sample$name
colnames(in_100m) <- df_sample$name
diag(in_100m) <- 0
for (i in 1:15) {
  for (j in i:15) {
    if (is.na(in_100m[i, j])==TRUE){
      i_name <- rownames(in_100m)[i]
      j_name <- colnames(in_100m)[j]
      in_100m[i, j] <- distHaversine(df_sample[i_name, 'coord'], df_sample[j_name, 'coord'])
      }
  }
}
in_100km <- in_100m/1000

# ---- Find ideal limit ----
limit <- in_100km[(in_100km>0)&(!is.na(in_100km))]
bound <- median(limit)
get_threshold <- function(bound){
  sum((in_100km>0)&(in_100km<bound)&(!is.na(in_100km)))
}
v <- c(12)
append(v, 1)

for (i in 1:round(max(limit), 0)){
  v[i] <- get_threshold(i)
}

plot(v)

# --> idk, now play with the median

# ---- API Call ---- 
library(httr)
library(jsonlite)
library(glue)

coord1 <- paste(df_sample[1, 'coord'][1], df_sample[1, 'coord'][2], sep=',')
coord2 <- paste(df_sample[2, 'coord'][1], df_sample[2, 'coord'][2], sep=',')
call <- glue('https://router.project-osrm.org/route/v1/driving/{coord1};{coord2}?overview=false')
route_planner <- GET(url = call)
status_code(route_planner)
fromJSON(content(route_planner, 'text', encoding='UTF-8'), flatten=TRUE)$routes$duration
fromJSON(content(route_planner, 'text', encoding='UTF-8'), flatten=TRUE)$routes$distance

# ---- Get distance and duration matrix ----
# same loop --> reduce API calls --> reduce time
# bound <- median(limit)
bound <- 70

dur_matrix <- matrix(NA, nrow=nrow(df_sample), ncol=nrow(df_sample))
rownames(dur_matrix) <- df_sample$name
colnames(dur_matrix) <- df_sample$name

dis_matrix <- matrix(NA, nrow=nrow(df_sample), ncol=nrow(df_sample))
rownames(dis_matrix) <- df_sample$name
colnames(dis_matrix) <- df_sample$name

pb <- txtProgressBar(min = 1, max = 15, initial = 0) 
stepi <- 0

st <- Sys.time()
iterations <- 0
for (i in 1:15){
  for (j in i:15){
    if((in_100km[i, j]<bound)&(i!=j)){
      coord1_n <- paste(df_sample[i, 'coord'][1], df_sample[i, 'coord'][2], sep=',')
      coord2_n <- paste(df_sample[j, 'coord'][1], df_sample[j, 'coord'][2], sep=',')
      call <- glue('https://router.project-osrm.org/route/v1/driving/{coord1_n};{coord2_n}?overview=false')
      # print(call)
      # print(paste('The distance of point', coord1, 'and the point', coord2, 'are calcaluting.'))
      route_planner_new <- GET(url=call)
      Sys.sleep(0.05)
      if(status_code(route_planner)==200){
        dur_matrix[i, j] <- fromJSON(content(route_planner_new, 'text', encoding='UTF-8'), flatten=TRUE)$routes$duration
        dis_matrix[i, j] <- fromJSON(content(route_planner_new, 'text', encoding='UTF-8'), flatten=TRUE)$routes$distance
      }
      else{
        dur_matrix[i, j] <- NA
        dis_matrix[i, j] <- NA
      }
    }
    else{
      dur_matrix[i, j] <- -1
      dis_matrix[i, j] <- -1
    }
    iterations <- iterations+1
    stepi <- stepi+1
    setTxtProgressBar(pb,stepi)
  }
  stepi <- 0
  close(pb)
}
diag(dur_matrix) <- 0
diag(dis_matrix) <- 0

end <- Sys.time()
end-st


(((3178*3178-3178)/2)*(16.56278/((15*15-15)/2)))/60/60/24

# ---- Save matrices ----
arrow::write_parquet(as.data.frame(dur_matrix), "data/dur_matrix.parquet")
arrow::write_parquet(as.data.frame(dis_matrix), "data/dis_matrix.parquet")


