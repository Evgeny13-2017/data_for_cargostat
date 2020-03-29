#-------------------Загрузка библиотек------------------------------#
setwd("C:/R")
library(readxl)
library(stringr)

#-------------------Загрузка биржевых сводок всемирного банка-------#
download.file(url=w.url<-"http://pubdocs.worldbank.org/en/245551480717940139/CMO-Historical-Data-Monthly.xlsx", 
              destfile = "C:/R/WB/WB.xlsx", mode = "wb")
wb<-read_excel(path = "C:/R/WB/WB.xlsx", sheet = "Monthly Prices")
wb_nc<-data.frame(CODE=unname(t(wb[6,2:83])[,1]),
                  UNIT=unname(t(wb[5,2:83])[,1]),
                  DESCR=unname(t(wb[4,2:83])[,1]),
                  stringsAsFactors = F)

wb_prices<-data.frame(
  PERIOD=character(),
  CODE=character(),
  PRICE=numeric(0),
  stringsAsFactors = F)
for (i in 2:83)
{
wb_prices<-rbind(wb_prices, 
      data.frame(
  PERIOD=paste0(str_sub(wb[7:nrow(wb),1],1,4),str_sub(wb[7:nrow(wb),1],6,7)),
  CODE=rep(wb[6,i],length(wb[7:nrow(wb),i])),
  PRICE=as.numeric(wb[7:nrow(wb),i]),
  stringsAsFactors = F))  
}
wb_prices<-wb_prices[is.na(wb_prices[,3])==F,]

#--------------Запись результатов в БД---------------#
i<-1
library(RMySQL)
library(progress)
conn = dbConnect(MySQL(), user='root', password='', dbname='cargostat', host='localhost')
pb <- progress_bar$new(total =  nrow(wb_prices))
for (i in (1:nrow(wb_prices)))
{
  pb$tick()  
  dbGetQuery(conn, paste0("Insert into wb_prices values ('",
                          wb_prices[i,"PERIOD"],"','",
                          wb_prices[i,"CODE"],"',",
                          wb_prices[i,"PRICE"],")")
  )
}

dbDisconnect(conn)





library(RMySQL)
library(progress)
conn = dbConnect(MySQL(), user='root', password='', dbname='cargostat', host='localhost')
pb <- progress_bar$new(total =  nrow(wb_prices))
for (i in (1:nrow(wb_nc)))
{
  pb$tick()  
  dbGetQuery(conn, paste0("Insert into wb_nc values ('",
                          wb_nc[i,"CODE"],"','",
                          wb_nc[i,"UNIT"],"','",
                          wb_nc[i,"DESCR"],"')")
  )
}

dbDisconnect(conn)









w<-read.xls(w.url, sheet="Monthly Prices", pattern="1960M01", header = FALSE)
w.url<-"http://pubdocs.worldbank.org/en/148131457048917308/CMO-Historical-Data-Monthly.xlsx"
h<-read.xls(w.url, sheet="Monthly Prices", pattern="Crude oil, average", header = FALSE, nrows=2)
h[1,1]<-"Period"
w[w==".."]<-NA
w[,-1]<-as.numeric(unlist(w[,-1]))
colnames(w)<-as.vector(t(h[1,]))
plot(w[610:681,9])
mean(w[610:681,9])
colnames(w)[9]

w[675:681,9:10]













conn = dbConnect(MySQL(), user='root', password='', dbname='cargostat', host='localhost')

joni<-dbGetQuery(conn, "select * from eurostat 
where declarant='003'
and partner='0616'
and product_nc='73MMM000'
and period='201602'
           ")