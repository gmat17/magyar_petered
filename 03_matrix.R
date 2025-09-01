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

n_cores <- detectCores()
cluster <- makeCluster(n_cores - 1) 
registerDoParallel(cluster)

coords2 <- c()
coords2 <- foreach(i = 1:nrow(df), .combine='rbind') %dopar% {
  coords2[i] <- paste(df[i, 'coord'][1],df[i, 'coord'][2], sep=',')
}
stopCluster(cl = cluster)

n_cores <- detectCores()
cluster <- makeCluster(n_cores - 1) 
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

fileConn<-file("request.txt")
writeLines(call, fileConn)
close(fileConn)

# I can get all the 3178 coordinates by rows
  # --> more efficient: avoid columns which are already have

9+90*2+900*3+9000*4+1*3*3178-1
nchar(call)

# ---- Visualise the 100 call problem ----
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

((3178*3178-3178)/2)/100/3.52/60/60
  # ((3178*3178-3178)/2) --> upper.tri(matrix)
  # /100 --> we can stack the munis by 100
  # /3.52 requests/sec after wrk -t1 -c1 -d120s 
  # /60 to mins
  # /60 to hours

((3178*3178-3178)/2)/10/40.99/60/60
  # ((3178*3178-3178)/2) --> upper.tri(matrix)
  # /100 --> we can stack the munis by 10
  # /40.99 requests/sec after wrk -t1 -c1 -d30s 
  # /60 to mins
  # /60 to hours
