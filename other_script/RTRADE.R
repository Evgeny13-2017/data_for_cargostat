#???????? ???????
#https://www.youtube.com/watch?v=kofPIbRMLH4
#R-treaks
#https://www.youtube.com/watch?v=Toc__W7L2Qo
#Price elasticity
#http://www.salemmarafi.com/code/price-elasticity-with-r/
#Eurostat tools
#https://cran.r-project.org/web/packages/eurostat/vignettes/eurostat_tutorial.pdf

#library(erer) #'Empirical Research in Economics:
#library(concordance)#r matching products in different classification codes
#library(oec) #e to obtain international trade data to create spreadsheets 
#library(eurostat)
#library(downloader)


url.es<-"http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=comext%2F201611%2Fdata%2Fnc201607.7z"
download.file(url.es,"nc201607.7z", mode = "wb")
my_batch <- "1.bat"
shell.exec(shQuote(paste(my_batch), type = "cmd"))
t3<-read.table("nc201607.dat", nrows=10, header=T, quote="\"", sep=",")


#t2<-get_eurostat("DS-645593")
#re<-search_eurostat("CN8", type = "dataset")


setwd("C:/R")

library(rJava)
library(xlsx)
library(gdata)
w.url<-"http://pubdocs.worldbank.org/en/148131457048917308/CMO-Historical-Data-Monthly.xlsx"
w<-read.xls(w.url, sheet="Monthly Prices", pattern="1960M01", header = FALSE)
w.url<-"http://pubdocs.worldbank.org/en/148131457048917308/CMO-Historical-Data-Monthly.xlsx"
h<-read.xls(w.url, sheet="Monthly Prices", pattern="Crude oil, average", header = FALSE, nrows=2)
h[1,1]<-"Period"
colnames(w)<-h[1,]
str(w)
plot(w[,2])


library(rjson)

string <- "http://comtrade.un.org/data/cache/partnerAreas.json"
reporters <- fromJSON(file=string)
reporters <- as.data.frame(t(sapply(reporters$results,rbind)))


#function

get.Comtrade <- function(url="http://comtrade.un.org/api/get?"
                         ,maxrec=50000
                         ,type="C"
                         ,freq="A"
                         ,px="HS"
                         ,ps="now"
                         ,r
                         ,p
                         ,rg="all"
                         ,cc="TOTAL"
                         ,fmt="json"
)
{
  string<- paste(url
                 ,"max=",maxrec,"&" #maximum no. of records returned
                 ,"type=",type,"&" #type of trade (c=commodities)
                 ,"freq=",freq,"&" #frequency
                 ,"px=",px,"&" #classification
                 ,"ps=",ps,"&" #time period
                 ,"r=",r,"&" #reporting area
                 ,"p=",p,"&" #partner country
                 ,"rg=",rg,"&" #trade flow
                 ,"cc=",cc,"&" #classification code
                 ,"fmt=",fmt        #Format
                 ,sep = ""
  )
  
  if(fmt == "csv") {
    raw.data<- read.csv(string,header=TRUE)
    return(list(validation=NULL, data=raw.data))
  } else {
    if(fmt == "json" ) {
      raw.data<- fromJSON(file=string)
      data<- raw.data$dataset
      validation<- unlist(raw.data$validation, recursive=TRUE)
      ndata<- NULL
      if(length(data)> 0) {
        var.names<- names(data[[1]])
        data<- as.data.frame(t( sapply(data,rbind)))
        ndata<- NULL
        for(i in 1:ncol(data)){
          data[sapply(data[,i],is.null),i]<- NA
          ndata<- cbind(ndata, unlist(data[,i]))
        }
        ndata<- as.data.frame(ndata)
        colnames(ndata)<- var.names
      }
      return(list(validation=validation,data =ndata))
    }
  }
}

##part 2: donwload specific data
#information:

#hs codes:
#Rice: 1006, 100610, 100620, 100630, 100640
#Wheat: 1001, 100190, 100110, 110100
#Soy: 1201, 120100, 120110, 120190
#Maize: 1005, 100510, 100590


#country codes:
#Argentina: 32
#China: 156
#India: 699
#Indonesia: 360
#Kazakhstan: 398
#Russian Federation: 643
#Ukraine: 804
#Viet nam: 704

#note: we can only download less than five countries and years at a time
#note: i am sure there is a more elegant/efficient way to do this
#note: do it line by line, it seems to crash if i try to do it all at once:
#start downloading:

q1a <- get.Comtrade(r="32,156,699,360", p="all", ps="2014,2013,2012,2011,2010", fmt="csv", cc="100510,100590")
q1b <- get.Comtrade(r="398,643,804,704", p="all", ps="2014,2013,2012,2011,2010", fmt="csv", cc="100510,100590")

q2a <- get.Comtrade(r="32,156,699,360", p="all", ps="2009,2008,2007,2006,2005", fmt="csv", cc="100510,100590")
q2b <- get.Comtrade(r="398,643,804,704", p="all", ps="2009,2008,2007,2006,2005", fmt="csv", cc="100510,100590")

q3a <- get.Comtrade(r="32,156,699,360", p="all", ps="2004,2003,2002,2001,2000", fmt="csv", cc="100510,100590")
q3b <- get.Comtrade(r="398,643,804,704", p="all", ps="2004,2003,2002,2001,2000", fmt="csv", cc="100510,100590")

#create data frame for each of this

dq1a <- as.data.frame(do.call(rbind, q1a))
dq1b <- as.data.frame(do.call(rbind, q1b))

dq2a <- as.data.frame(do.call(rbind, q2a))
dq2b <- as.data.frame(do.call(rbind, q2b))

dq3a <- as.data.frame(do.call(rbind, q3a))
dq3b <- as.data.frame(do.call(rbind, q3b))


#part 3: export data
#append all data frames

append=do.call(rbind, list(dq1a, dq1b, dq2a, dq2b, dq3a,dq3b))

##part 3: export files files

#connect to data base mysql
Sys.setlocale("LC_CTYPE", "en_RU.UTF-8")

eu$DECLARANT<-as.character(eu$DECLARANT)
eu$PRODUCT_NC<-as.character(eu$PRODUCT_NC)
eu_<-head(eu)
library(RMySQL)
conn = dbConnect(MySQL(), user='root', password='', dbname='cargostat', host='localhost')
dbGetQuery(conn, 'set character set utf8')
dbSendQuery(conn,'SET NAMES utf8') 
OKSM <- dbGetQuery(conn, "select  * from tymyeddfg")
dbGetQuery(con, "Insert into goods values ('','Банан', '56')")

dbWriteTable(conn, "cs_temp", eu_)



library(progress)
dbDisconnect(conn)
conn = dbConnect(MySQL(), user='root', password='', dbname='cargostat', host='localhost')
pb <- progress_bar$new(total =  nrow(eu))
for (i in (1:nrow(eu)))
  {
  pb$tick()  
     dbGetQuery(conn, paste0("Insert into eurostat values ('",eu[i,1],"','",eu[i,2],
"','",eu[i,3],"','",eu[i,4],"','",eu[i,5],"','",eu[i,6],"',",eu[i,7],",",eu[i,8],",",
ifelse(is.na(eu[i,9])==T,"null", eu[i,9])
,")")
)
}
     





a <- c('ывапывап','ывапывап','ывапывап')
#Encoding(a) <- rep("UTF-8",length(a))

b <- c(89,77,95)
c <- c('','','')
d <- data.frame(c,a,b)
#dbGetQuery(con, 'set character set utf8')
dbSendQuery(con,'SET NAMES utf8') 
dbWriteTable(con,'her',d,overwrite = TRUE)
dbGetQuery(con, "Select c,a,b from her")

dbGetQuery(con, "Insert into goods select c,a,b from her")








download.file(url = "http://media.portblue.net/resources/uncomtrade-r-package/comtrade.tar.gz", destfile = "comtrade.tar.gz")
library(tools)
md5sum("comtrade.tar.gz")



