# ---- Read-in file and create coordinate column ----
df=read.csv('data/municipality_lonlat.csv')
df$coord <- as.matrix(df[,c('x', 'y')])
rownames(df) <- 1:nrow(df)

# ---- Take a sample from the whole dataframe ----
set.seed(2*17)
df_sample <- df[sample(rownames(df), size=15, replace=FALSE),]
rownames(df_sample) <- df_sample$name

# ---- Create in_100km matrix ----
library(geosphere)

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

# ---- API Call ----
coords <- c()
for (i in rownames(df_sample)){
  coords <- c(coords, paste(df_sample[i, 'coord'][1],df_sample[i, 'coord'][2], sep=','))
}
api_coords <- paste(coords, collapse=';')

# distance matrix
call <- glue('http://localhost:8080/table/v1/driving/{api_coords}?annotations=distance')
route_planner <- GET(url = call)
status_code(route_planner)
dis_matrix <- fromJSON(content(route_planner, 'text', encoding='UTF-8'), flatten=TRUE)$distances/1000
rownames(dis_matrix) <- rownames(df_sample)
colnames(dis_matrix) <- rownames(df_sample)

# duration matrix
call <- glue('http://localhost:8080/table/v1/driving/{api_coords}?annotations=duration')
route_planner <- GET(url = call)
status_code(route_planner)
dur_matrix <- fromJSON(content(route_planner, 'text', encoding='UTF-8'), flatten=TRUE)$duration
rownames(dur_matrix) <- rownames(df_sample)
colnames(dur_matrix) <- rownames(df_sample)

heatmap(dur_matrix)

arrow::write_parquet(as.data.frame(dur_matrix), "data/dur_matrix2.parquet")
arrow::write_parquet(as.data.frame(dis_matrix), "data/dis_matrix2.parquet")

# ---- Compare matrices ----
dur1 <- arrow::read_parquet('data/dur_matrix.parquet')
dur2 <- arrow::read_parquet('data/dur_matrix2.parquet')
# two seconds difference --> negligable

# conclusion: keep /table view

# ---- Table service from API ----
n_cores <- detectCores()
cluster <- makeCluster(n_cores - 1) 
registerDoParallel(cluster)
coords2 <- c()
coords2 <- foreach(i = 1:nrow(df_sample), .combine='rbind') %dopar% {
  coords2[i] <- paste(df[i, 'coord'][1],df[i, 'coord'][2], sep=',')
}
stopCluster(cl = cluster)
n <- nrow(df_sample)
api_coords <- paste(coords2, collapse=';')
dis_matrix <- matrix(NA, n, n)
rownames(dis_matrix) <- df_sample$name
colnames(dis_matrix) <- df_sample$name
end_counter <- 0

# registerDoParallel(cluster)
for (i in 1:n) {
  call <- paste('http://localhost:8080/table/v1/driving/',api_coords,
                '?annotations=distance&sources=',i-1,
                '&destinations=',paste(seq(i-1,n-1,1), collapse=';'), sep='')
  print(paste('The',i,'th call begun!'))
  route_planner <- httr::GET(url = call)
  if(status_code(route_planner)==200){
    dis_matrix[i,i:n] <- fromJSON(content(route_planner, 'text', encoding='UTF-8'), flatten=TRUE)$distances
  }
}

for (i in seq(1,n,3)) {
  if (i == n-3){
    call <- paste('http://localhost:8080/table/v1/driving/',api_coords,
                  '?annotations=duration&sources=',i-1,
                  '&destinations=',paste(seq(i-1,n-1,1), collapse=';'), sep='')
    print(paste('Call',i))
    route_planner <- httr::GET(url = call)
    if(status_code(route_planner)==200){
      dur_matrix[i,i:n] <- fromJSON(content(route_planner, 'text', encoding='UTF-8'), flatten=TRUE)$duration
    }
    else{
      print(paste(i, 'th call did not give any data.', sep=''))
      break
    }
  }
  else{
    call <- paste('http://localhost:8080/table/v1/driving/',api_coords,
                  '?annotations=duration&sources=',paste(seq(i-1,i+1,1), collapse=';'),
                  '&destinations=',paste(seq(i-1,n-1,1), collapse=';'), sep='')
    print(paste('Call ',i,'-',i+3, sep=''))
    route_planner <- httr::GET(url = call)
    if(status_code(route_planner)==200){
      dur_matrix[i:i+2,i:n] <- fromJSON(content(route_planner, 'text', encoding='UTF-8'), flatten=TRUE)$duration
    }
    else{
      print(paste('Call ',i,'-',i+2, ' did not give any data.', sep=''))
      break
    }
  }
}