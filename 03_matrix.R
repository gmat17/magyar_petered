# ---- Read-in file and create coordinate column ----
df=read.csv('data/municipality_lonlat.csv')
df$coord <- as.matrix(df[,c('x', 'y')])
rownames(df) <- df$name

# ---- Create air matrix ----
library(geosphere)
library(foreach)
library(doParallel)

n <- nrow(df)
air_matrix <- matrix(NA,n,n)
rownames(air_matrix) <- df$name
colnames(air_matrix) <- df$name
object.size(air_matrix)

distHaversine(df[1,'coord'], df[1:10,'coord'])

n_cores <- detectCores()
cluster <- makeCluster(n_cores - 1) 
registerDoParallel(cluster)

res <- foreach(i = 1:n, .combine=rbind, .packages='geosphere') %dopar% {
  dists <- numeric(n)
  dists[i:n] <- distHaversine(df[i,'coord'], df[i:n,'coord'])
  return(dists)
}

stopCluster(cl = cluster)

diag(air_matrix) <- 0
air_matrix[upper.tri(air_matrix, diag=FALSE)] <- res[upper.tri(res, diag=FALSE)]
air_matrix[lower.tri(air_matrix, diag=FALSE)] <- res[upper.tri(res, diag=FALSE)]
air_matrix <- air_matrix/1000
object.size(air_matrix)
rm(res)

# ---- API Call ----
library(httr)
library(jsonlite)
library(glue)

registerDoParallel(cluster)
coords2 <- c()
coords2 <- foreach(i = 1:nrow(df), .combine='rbind') %dopar% {
  coords2[i] <- paste(df[i, 'coord'][1],df[i, 'coord'][2], sep=',')
}
stopCluster(cl = cluster)

# --- Limit of API ---

registerDoParallel(cluster)
max_num <- 3178
foreach (i=c(2000, 3178)) %dopar% {
  api_coords <- paste(coords2[1:i], collapse=';')
  call <- paste('http://localhost:8080/table/v1/driving/',api_coords,'?annotations=distance&sources=0', sep='')
  route_planner <- httr::GET(url = call)
  if(httr::status_code(route_planner)!=200){
    print(paste('The API reached its maximum level at', i))
    break
  }
  if(i==max_num){
    print(paste("The API didn't reach its maximum level up to", i))
    }
  }
stopCluster(cl = cluster)

# to call it from browser as well
fileConn<-file("request.txt")
writeLines(call, fileConn)
close(fileConn)

# I can get all the 3178 coordinates by rows
  # --> more efficient: avoid columns which are already had

# ---- Build distance matrix ----
# simple solution --> just to show the result to Laci --> later possible devs

n <- nrow(df)
api_coords <- paste(coords2, collapse=';')
dis_matrix <- matrix(NA, n, n)
rownames(dis_matrix) <- df$name
colnames(dis_matrix) <- df$name
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
  else{
    print(paste(i, 'th call did not give any data.', sep=''))
    break
  }
}
arrow::write_parquet(as.data.frame(dis_matrix), "data/dis_matrix_full.parquet")

# ---- Build duration matrix ----

n <- nrow(df)
api_coords <- paste(coords2, collapse=';')
dur_matrix <- matrix(NA, n, n)
rownames(dur_matrix) <- df$name
colnames(dur_matrix) <- df$name
end_counter <- 0

for (i in seq(1,n,3)) {
  if (i >= n-3){
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
    print(paste('Call ',i,'-',i+2, sep=''))
    route_planner <- httr::GET(url = call)
    if(status_code(route_planner)==200){
      dur_matrix[i:(i+2),i:n] <- fromJSON(content(route_planner, 'text', encoding='UTF-8'), flatten=TRUE)$duration
    }
    else{
      print(paste('Call ',i,'-',i+2, ' did not give any data.', sep=''))
      break
    }
  }
}

arrow::write_parquet(as.data.frame(dur_matrix), "data/dur_matrix_full.parquet")

# ---- Visualise the 100 call problem ----
# skippable but the ggplot is quite nice
v <- c()
for(i in 1:30){
  v <- c(v, max(rowSums(air_matrix<i)))
}

coord_limit_problem <- data.frame(
  km = seq_along(v),
  number_of_munis = v
)

# Plot
ggplot(coord_limit_problem, aes(x = km, y = number_of_munis)) +
  geom_line(color = "blue", size = 1) +
  geom_point(color = "darkblue", size = 2) +
  geom_hline(yintercept = 100, color = "red", linetype = "dashed", size = 1) +
  theme_minimal() +
  labs(
    title = "Line Plot with Threshold",
    x = "Kilometers",
    y = "Number of municipalities"
  )
max(coord_limit_problem[coord_limit_problem$number_of_munis<100,'km'])
# 19 --> the maximal km in air --> assumption: nobody travelled more than 19 km in air 
# to hear magyar peter in live
