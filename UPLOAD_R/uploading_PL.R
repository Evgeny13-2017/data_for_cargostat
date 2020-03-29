#----------------------------------------------------#
#------------- Upload Public Ledger data ------------#
#----------------------------------------------------#
library(RMySQL)
conn = dbConnect(MySQL(), 
                 user='cb74929_cargo',
                 password='selmed45', 
                 dbname='cb74929_cargo', 
                 host='92.53.96.170')
#Set current year here
dbGetQuery(conn, "delete from cb74929_cargo.PL 
                  where substr(period,1,4) = '2017'") 
pl_descr <- dbGetQuery(conn, "select * from cb74929_cargo.PL_DESCR")
dbDisconnect(conn)
detach(name = "package:RMySQL", unload=TRUE)

library(sqldf)
library(readxl)
pl <- read_excel("C:/R/PL/pl.xls")
pl$group <- NA
for (i in 1:nrow(pl)) 
{
  if (substr(pl[i,1],1,8)=="Product:") 
  {pl[i,"group"] <- pl[i,1]} else
  {pl[i,"group"] <- NA}
}
pl[1,"group"]<-"dummy"
for (i in 1:nrow(pl)) 
{
  if (is.na(pl[i,"group"]) == TRUE)
  {pl[i,"group"] <- pl[i-1,"group"]} else
  {pl[i,"group"] <- pl[i,"group"]}
}
pl<-pl[is.na(pl[,2]) == FALSE,]
pl$period <- as.numeric(substr(colnames(pl)[1],nchar(colnames(pl)[1])-3, nchar(colnames(pl)[1])))
colnames(pl) <- c("MONTH","MAX_PRICE","MIN_PRICE","PRODUCT","YEAR")
pl <- sqldf('select product, year, month, max_price, min_price from pl
             where max_price not like "%price%"')
pl$PRODUCT <- substr(pl$PRODUCT,12,150)
pl$MAX_PRICE <- as.numeric(pl$MAX_PRICE)
pl$MIN_PRICE <- as.numeric(pl$MIN_PRICE)
mnth <- data.frame(MONTH = c("Jan","Feb","March","Apr","May","June",
                             "Jul","Aug","Sep","Oct","Nov","Dec"),
                       MM = c("01","02","03","04","05","06",
                              "07","08","09","10","11","12"),
                   stringsAsFactors = F)
pl <- merge(x = pl, y = mnth, by = "MONTH")
pl <- merge(x = pl, y = pl_descr, by = "PRODUCT")
pl <- data.frame(PERIOD = paste0(pl$YEAR, pl$MM),
                       PGROUP = pl$PGROUP,
                       PRODUCT = pl$PRODUCT,
                       MIN_PRICE = pl$MIN_PRICE,
                       MAX_PRICE = pl$MAX_PRICE,
                       stringsAsFactors = F)
pl <- sqldf('select * from pl
                  order by period, pgroup, product
                  ')
library(RMySQL)
conn = dbConnect(MySQL(), 
                 user='cb74929_cargo',
                 password='selmed45', 
                 dbname='cb74929_cargo', 
                 host='92.53.96.170')
dbWriteTable(conn, "PL", pl, overwrite = F, append = T, row.names = F)
dbGetQuery(conn, "select substr(period,1,4) PERIOD, count(*) N
           from cb74929_cargo.PL
group by substr(period,1,4) 
           ")
dbDisconnect(conn)