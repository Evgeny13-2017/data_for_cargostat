#Doing Business reports
#library(eurostat)
#-------------------Загрузка библиотек------------------------------#
setwd("C:/R")
library(sqldf)
library(rJava)
library(gdata)
library(countrycode)
library(xlsx)

#------------------Передача данных в БД----------------------------#
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

detach(name = "package:RMySQL", unload=TRUE)









#-------------------Загрузка баз NSI--------------------------------#
OKSM<-read.csv("C:/R/NSI/OKSM.csv", sep=";", as.is = T,
               colClasses="character")
OKSM[200,"ALFA2"]<-"NA"
ifelse(  nrow( sqldf('select alfa2, count(*) from OKSM group by alfa2 having count(*)>1 '))==0, 
"КОнтроль четности ОКСМ пройден", "Требуется проверка ОКСМ")
eu_rep<-read.xlsx("C:/R/EU/rep.xlsx",sheetIndex = 1, colClasses = "character")


country_class<-countrycode_data
colnames(country_class)[1]<-"country_name"
setwd("C:/R/EU")
rep<-read.table("Reporters.txt")
rep<-read.csv("C:/R/EU/Partners.txt", sep = "\t", header = F)
ksm<-sqldf('select 
o.ALFA2,
o.ALFA3,
o.CODE,
o.EU_REP EU_REPORTER,
r.EU_PARTNER,
o.SHORTNAME NAME_RU,
o.FULLNAME,
c.COUNTRY_NAME NAME_EN,
o.MACROREGION CONTINENT_RU,
o.SUBREGION REGION_RU,
c.CONTINENT CONTINENT_EN,
c.REGION REGION_EN
from OKSM o left join country_class c on o.ALFA2=c.ISO2C
            left join eu_rep r on r.ALFA2=o.ALFA2',
method = "character")
str(ksm)
ksm[ksm[,"REGION_RU"]=="TBD","REGION_RU"]<-""
write.csv(ksm, "C:/R/countries.csv")


library(progress)
dbDisconnect(conn)
# 
# conn = dbConnect(MySQL(), user='root', password='', dbname='cargostat', host='localhost')
# pb <- progress_bar$new(total =  nrow(ksm))
# i<-15
# for (i in (1:nrow(ksm)))
# {
#   pb$tick()  
#   dbGetQuery(conn, paste0("Insert into countries values ('",
#                           ksm[i,1],"','",
#                           ksm[i,2],"','",
#                           ksm[i,3],"','",
#                           ifelse(is.na(ksm[i,4])==T,"",ksm[i,4]),"','",
#                           ifelse(is.na(ksm[i,5])==T,"",ksm[i,5]),"','",
#                           ifelse(is.na(ksm[i,6])==T,"",ksm[i,6]),"','",
#                           ifelse(is.na(ksm[i,7])==T,"",ksm[i,7]),"','",
#                           ifelse(is.na(ksm[i,8])==T,"",ksm[i,8]),"','",
#                           ifelse(is.na(ksm[i,9])==T,"",ksm[i,9]),"','",
#                           ifelse(is.na(ksm[i,10])==T,"",ksm[i,10]),"','",
#                           ifelse(is.na(ksm[i,11])==T,"",ksm[i,11]),"','",
#                           ifelse(is.na(ksm[i,12])==T,"",ksm[i,12]),"'",
#                           ")")
#   )
# }


countr<-dbGetQuery(conn, "select * from countries")

detach(name = "package:RMySQL", unload=TRUE)



library(RMySQL)
dbDisconnect(conn)
conn = dbConnect(MySQL(), user='root', password='', dbname='cargostat', host='localhost')
dbGetQuery(conn, paste0("Insert into countries values ('1','2','3','','5','6','7','8','9','10','11','12')"))

dbGetQuery(conn, paste0("Insert into countries values ('AW','ABW','533','','0474','АРУБА',
                        '','Aruba','Северная и Южная Америка','Карибский бассейн',
                        'Americas','Caribbean')"))




#-------------------Загрузка отчета Doing businness-----------------#
download.file(url="http://databank.worldbank.org/data/download/DB_CSV_en.zip", destfile = "C:/R/DoB/DB_CSV_en.zip", mode = "wb")
my_batch <- "C:/R/DoB/unzip.bat"
shell.exec(shQuote(paste(my_batch), type = "cmd"))
Sys.sleep(5)
dob<-read.csv("C:/R/DoB/DB_Data.csv")


#-------------------Загрузка отчета Logistic Performance Index------#
download.file(url="http://lpi.worldbank.org/sites/default/files/International_LPI_from_2007_to_2016.xlsx", destfile = "C:/R/DoB/LPI.xlsx", mode = "wb")
LPI.URL<-"http://lpi.worldbank.org/sites/default/files/International_LPI_from_2007_to_2016.xlsx"
LPI<-read.xlsx(file = "C:/R/DoB/LPI.xlsx", sheetName = "2016")


#-------------------Загрузка биржевых сводок всемирного банка-------#
w.url<-"http://pubdocs.worldbank.org/en/148131457048917308/CMO-Historical-Data-Monthly.xlsx"
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
#-------------------Загрузка данных Евростат-----------------------#
month<-"201601"

url.eu<-paste0("http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=comext%2F201612%2Fdata%2Fnc",month,".7z")
download.file(url=url.eu, destfile = paste0("C:/R/EU/nc",month,".7z"), mode = "wb")
my_batch <- "C:/R/EU/unzip.bat"
shell.exec(shQuote(paste(my_batch), type = "cmd"))
Sys.sleep(20)
ifelse(file.exists(paste0("C:/R/EU/nc",month,".dat")), paste0("OKAY, ",month," archive ready"), Sys.sleep(20))
ifelse(file.exists(paste0("C:/R/EU/nc",month,".dat")), "OKAY, going to read it!", "Can't find archive!!!")
#unlink("C:/R/EU/nc201607.7z")
eu<-read.table(paste0("C:/R/EU/nc",month,".dat"), header=T, quote="\"", sep=",", as.is = T,
               colClasses=c(rep("character",6), rep("numeric",3))
               )
eu_<-head(eu)



es<-sqldf('select o.alfa2, sum(QUANTITY_TON) ton from eu join OKSM o on eu.declarant=o.eu_rep group by o.alfa2')
http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=comext%2F201612%2Fdata%2Fnc201608.7z

#-------------------Загрузка статистики Республики Казахстан-------#
month.rk<-"201610"
url.rk<-paste0("http://kgd.gov.kz/sites/default/files/exp_trade/ts_10z_",
               substr(month.rk,5,7), "_",
               substr(month.rk,3,4), ".zip")
download.file(url=url.rk, destfile = paste0("C:/R/RK/rk",month.rk,".zip"), mode = "wb")
my_batch <- "C:/R/RK/unzip.bat"
shell.exec(shQuote(paste(my_batch), type = "cmd"))
Sys.sleep(10)
ifelse(file.exists(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
                          "_",substr(month.rk,3,4),
                          ".xlsx")), paste0("OKAY, ",month.rk," archive ready"), 
       Sys.sleep(20))

ifelse(file.exists(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
                          "_",substr(month.rk,3,4),
                          ".xlsx")), paste0("OKAY, goung to read ",month.rk," archive"), 
       "Something wrong")
intro.dat: UTF-8 Unicode (with BOM) text
rk<-read.xls(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
                    "_",substr(month.rk,3,4),
                    ".xlsx"), sheet="PRINT", header = FALSE, fileEncoding="UTF-8-BOM")
#, fileEncoding="UTF-16")
"UTF-8-BOM"

iconv(rk, to = "UTF-8")
iconv(rk, to = "UTF-8")



options(java.parameters = "-Xmx2012m")

res <- read.xlsx(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
                        "_",substr(month.rk,3,4),
                        ".xlsx"), 1)  # read the second sheet


wb <- loadWorkbook(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
                          "_",substr(month.rk,3,4),
                          ".xlsx"))

rk1<-read.xlsx(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
                 "_",substr(month.rk,3,4),
                 ".xlsx"))

rka<-paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
       "_",substr(month.rk,3,4),
       ".xlsx")

wb <- loadWorkbook(system.file(rka))
cccc

rk<-read.xlsx(rka)

#df3 <- read.xlsx(wb, sheet = 2, skipEmptyRows = FALSE, colNames = TRUE)
#df4 <- read.xlsx(xlsxF


iconv(rk, to = "UTF8")          ".xlsx"), 1)  # read the second sheet
Error in .jcall("RJavaTools", "Ljava/lang/Object;", "invokeMethod", 
iconv(rk, to = "UTF8")
Sys.setlocale(,"ru_RU")




detach(name = "package:xlsx", unload=TRUE)

library(XLConnect)

res <- read.xlsx(paste0("C:/R/RK/RK.xlsx"), 1)  # read the second sheet
require(XLConnect)
data <- readWorksheetFromFile("C:/R/RK/RK.xlsx", sheet = 1)

require(RODBC)
conn = odbcConnectExcel("C:/R/RK/RK.xlsx") # open a connection to the Excel file
sqlTables(conn)$TABLE_NAME # show all sheets
df = sqlFetch(conn, "Sheet1") # read a sheet
df = sqlQuery(conn, "select * from [Sheet1 $]") # read a sheet (alternative SQL sintax)
close(conn) # close the connection to the file




install.packages("readxl")
library("readxl")
my_data <- read_excel("C:/R/RK/TS_10z_10_16.xlsx")  
rk <- read_excel("C:/R/RK/TS_10z_10_16.xlsx", sheet = "PRINT",  col_names = F, skip=7)
rk$g33<-rk[,1]

library(progress)
pb <- progress_bar$new(total =  nrow(rk))
for (i in 1:nrow(rk))
{
  pb$tick()
  rk[i,"g33"]<-ifelse(is.na(rk[i,"g33"]), rk[i-1,"g33"], rk[i,"g33"])
}
  
#-------------------Работа с картой мира---------------------------#
library(rworldmap)
sPDF <- joinCountryData2Map(es, 
                             joinCode = "ISO2",
                             nameJoinColumn = "ALFA2")
mapCountryData(sPDF, nameColumnToPlot='ton')

#https://www.students.ncl.ac.uk/keith.newman/r/maps-in-r
library(maps)
map('worldHires',
    c('UK', 'Ireland', 'Isle of Man','Isle of Wight'),
    xlim=c(-11,3), ylim=c(49,60.9))
points(-1.615672,54.977768,col=2,pch=18)
map('worldHires','Italy')

install.packages("maps")
install.packages("mapdata")
library(maps)
library(mapdata)

plot(map('worldHires'), pch=20, cex=.2)

template: tmpl.tex
\usepackage[russian]{babel}


save.image(file="ws.RData")




#Среднемесячные цены на отдельные товары
#http://economy.gov.ru/minec/activity/sections/foreigneconomicactivity/monitoring/20161011_6

# Конкуренты
# http://complex.imexp.ru/analytics
# пример отчета
# http://www.rusexporter.ru/research/industry/detail/4504/
# пример интерфейса
# http://www.rusexporter.ru/research/demand/
