library(RMySQL)
library(progress)
conn = dbConnect(MySQL(), user='root', password='', dbname='cargostat', host='localhost')
rk <- dbReadTable(conn, 'rk')
sq <- 1:nrow(rk)
sqm <- split(sq, ceiling(seq_along(sq)/100000))
conn = dbConnect(MySQL(), 
                 user='cb74929_cargo',
                 password='selmed45', 
                 dbname='cb74929_cargo', 
                 host='92.53.96.170')
pb <- progress_bar$new(total =  length(sqm))
for (i in 1:length(sqm)) {
  pb$tick()
  n <- sqm[[i]]
rk_split <- rk[n,]
dbWriteTable(conn, "RK", rk_split, overwrite = F, append = T, row.names = F)
}
