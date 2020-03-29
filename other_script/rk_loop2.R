library(sqldf)
library(readxl)
library(progress)

rk.total <- data.frame(PARTNER = character(0), PRODUCT_NC = character(0), FLOW = character(0), 
                       PERIOD = character(0), VALUE = numeric(0), QUANTITY = numeric(0),
                       SUP_QUANTITY = numeric(0), SUP_UOM = character(0), COUNTRY_FLAG = numeric(0),
                       stringsAsFactors = F)



for (p in c(201301:201312,
            201401:201412,
            201501:201512,
            201601:201612
             
            ))

  
  {
  month.rk<-p
  
  #month.rk<-200901

print(paste0("Block ",p, " started at ", date()))
url.rk<-paste0("http://kgd.gov.kz/sites/default/files/exp_trade/ts_10z_",
               substr(month.rk,5,7), "_",
               substr(month.rk,3,4), ".zip")
download.file(url=url.rk, destfile = paste0("C:/R/RK/rk",month.rk,".zip"), mode = "wb")
my_batch <- "C:/R/RK/unzip.bat"
shell.exec(shQuote(paste(my_batch), type = "cmd"))
Sys.sleep(10)

# ifelse(file.exists(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
#                           "_",substr(month.rk,3,4),
#                           ".xlsx")), paste0("OKAY, ",month.rk," archive ready"), 
#        Sys.sleep(20))
# 
# ifelse(file.exists(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
#                           "_",substr(month.rk,3,4),
#                           ".xlsx")), paste0("OKAY, going to read ",month.rk," archive"), 
#        print("Something wrong"))



if (file.exists(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
                       "_",substr(month.rk,3,4),
                       ".xlsx")))

{
  
  res<-try(
  read_excel(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
                          "_",substr(month.rk,3,4),
                          ".xlsx"), sheet = "Лист1"
                     ,  col_names = F, skip=7)
  ,silent = TRUE)
  
  if (class(res) == "try-error")
  {
    
    rk <- read_excel(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
                      "_",substr(month.rk,3,4),
                      ".xlsx"), sheet = "PRINT"
               ,  col_names = F, skip=7)
    
  } else {

    
    rk <- read_excel(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
                      "_",substr(month.rk,3,4),
                      ".xlsx"), sheet = "Лист1"
               ,  col_names = F, skip=7)    
    
  }
  unlink(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),"_",
                substr(month.rk,3,4),".xlsx"))
  
  }
  

if (file.exists(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
                       "_",substr(month.rk,3,4),
                       ".xls")))
  
{
  
  res<-try(
    read_excel(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
                      "_",substr(month.rk,3,4),
                      ".xls"), sheet = "Лист1"
               ,  col_names = F, skip=7)
    ,silent = TRUE)
  
  if (class(res) == "try-error")
  {
    
    rk <- read_excel(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
                            "_",substr(month.rk,3,4),
                            ".xls"), sheet = "PRINT"
                     ,  col_names = F, skip=7)
    
  } else {
    
    
    rk <- read_excel(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),
                            "_",substr(month.rk,3,4),
                            ".xls"), sheet = "Лист1"
                     ,  col_names = F, skip=7)    
    
  }
  
  
  
  
  unlink(paste0("C:/R/RK/TS_10z_",substr(month.rk,5,7),"_",
                substr(month.rk,3,4),".xls"))
  
  
  }


unlink(paste0("C:/R/RK/rk",month.rk,".zip"))

rk[rk=="-"]<-NA
rk[is.na(rk[,4]),4]<-0
rk[is.na(rk[,6]),6]<-0
rk[is.na(rk[,7]),7]<-0
rk[is.na(rk[,9]),9]<-0
rk[is.na(rk[,5])==F & rk[,5]==0,5]<-NA
rk[is.na(rk[,8])==F & rk[,8]==0,8]<-NA

for (i in 4:9) {rk[,i]<-round(as.numeric(rk[,i]),7)}
rk[is.na(rk[,1])==T,"COUNTRY_FLAG"]<-1

pb <- progress_bar$new(total =  nrow(rk))
for (i in 1:nrow(rk))
{
  pb$tick()
  rk[i,1]<-ifelse(is.na(rk[i,1]), rk[i-1,1], rk[i,1])
  rk[i,3]<-ifelse(is.na(rk[i,3]), rk[i-1,3], rk[i,3])
}

rk_ex<-data.frame(rk[rk[,4]>0 | rk[,6]>0, c(1,2,3,4,5,6,10)], "EX")
colnames(rk_ex)<-c("G33", "G34", "DEI", "G38T", "G31_7", "G46", "COUNTRY_FLAG", "DIR")
rk_im<-data.frame(rk[rk[,7]>0 | rk[,9]>0, c(1,2,3,7,8,9,10)], "IM")
colnames(rk_im)<-c("G33", "G34", "DEI", "G38T", "G31_7", "G46", "COUNTRY_FLAG", "DIR")
rk_<-rbind(rk_ex, rk_im)
rk_$DIR<-as.character(rk_$DIR)
rm(rk_ex, rk_im, rk)
rk_$PERIOD<-month.rk

rk.total<-rbind(rk.total,
                data.frame(PARTNER = rk_$G34, PRODUCT_NC = rk_$G33, FLOW = rk_$DIR, 
                           PERIOD = month.rk, VALUE = rk_$G46, QUANTITY = rk_$G38T,
                           SUP_QUANTITY = rk_$G31_7, SUP_UOM = rk_$DEI, 
                           COUNTRY_FLAG = rk_$COUNTRY_FLAG,
                           stringsAsFactors = F)
)

print(paste0("Block ",p, " finished at ", date()))
}




rkt<-rk.total

sqldf('select period, flow, country_flag, count(*), sum(value) val, sum(quantity) qty,
round(sum(value)/sum(quantity),2) pr
      from rkt
where 1=1
and flow="IM"
and country_flag is null
      group by period, flow, country_flag
      order by period, flow'
      )




prt<-sqldf('select partner, count(*)
      from rkt
where 1=1
and country_flag is not null
      group by partner
      order by 2 desc'
)


rkt<-rk.total[is.na(rk.total$COUNTRY_FLAG)==F,]
cntr<-aggregate(rkt$PRODUCT_NC, by=list(rkt$PARTNER), FUN = function(x){NROW(x)})


library(RMySQL)
conn = dbConnect(MySQL(), user='root', password='', dbname='cargostat', host='localhost')

#----------- Справочник стран РК ----------------------------------#
countries_rk<-read.csv("C:/R/NSI/countries_rk.csv", sep = ";", stringsAsFactors = F)
colnames(countries_rk)<-c("PARTNER","CODE")
countries_rk[87,2]<-"TBD_SС"
countries_rk[158,2]<-"NA"
countries_rk[221,2]<-"TBD_SG"
countries_rk[234,2]<-"TBD_AN"
countries_rk[240,2]<-"TBD_NO"
countries_rk[251,2]<-"TBD_DA"
countries_rk[258,2]<-"TBD_PT"
countries_rk[263,2]<-"TBD_VS"
countries_rk[268,2]<-"TBD_FJ"
countries_rk[269,2]<-"TBD_UJ"
#------------------------------------------------------------------#

#---------------- Загрузка кодов стран в набор данных РК ----------#

rk_codes <- data.frame(PARTNER = character(0), PRODUCT_NC = character(0), FLOW = character(0), 
                       PERIOD = character(0), VALUE = numeric(0), QUANTITY = numeric(0),
                       SUP_QUANTITY = numeric(0), SUP_UOM = character(0), COUNTRY_FLAG = numeric(0),
                       CODE = character(0), 
                       stringsAsFactors = F)


library(progress)
pb <- progress_bar$new(total =  length(c(200501:200512,
                                         200601:200612,
                                         200701:200712,
                                         200801:200812,
                                         200901:200912,
                                         201001:201012,
                                         201101:201112,
                                         201201:201212,
                                         201301:201312,
                                         201401:201412,
                                         201501:201512,
                                         201601:201611
)))

for (p in c(200501:200512,
            200601:200612,
            200701:200712,
            200801:200812,
            200901:200912,
            201001:201012,
            201101:201112,
            201201:201212,
            201301:201312,
            201401:201412,
            201501:201512,
            201601:201611
))
  
{
  pb$tick()
  rk_codes <- rbind(rk_codes, merge(x = rkt[rkt[,"PERIOD"]==p,], 
                        y = countries_rk, by = "PARTNER", all.x = TRUE))
  
}

rk_codes_copy<-rk_codes
rk_codes<-rk_codes_copy[is.na(rk_codes_copy[,"COUNTRY_FLAG"])==F,]
rownames(rk_codes)<-1:nrow(rk_codes)
tf<-rownames(rk_codes[rk_codes$PRODUCT_NC=="9209997000" & 
                                        rk_codes$PERIOD==200901
                                      ,])

rk_codes<-rk_codes[-c(1907399, 1908361, 1908444, 1908541, 1909079, 1909551, 1910102,
               1915387, 1918718, 1919840, 1921637, 1921650, 1922372, 1922704,
               1922727, 1922839, 1924940, 1925311, 1925794, 1925810, 1925819,
               1925826, 1926361, 1926428, 1927833, 1927978, 1929064, 1930175,
               1930180, 1930190, 1930270, 1930406, 1930540, 1930580, 1933028,
               1933111, 1933660, 1933738, 1933909),]
#------------------------------------------------------------------#



i<-7
library(RMySQL)
library(progress)
conn = dbConnect(MySQL(), user='root', password='', dbname='cargostat', host='localhost')
pb <- progress_bar$new(total =  nrow(rk_codes))
for (i in (1:nrow(rk_codes)))
{
  pb$tick()  
  dbGetQuery(conn, paste0("Insert into rk values (",
                          ifelse(is.na(rk_codes[i,"CODE"])==T,"null", paste0("'",rk_codes[i,"CODE"],"'"))
                          ,",'",
#                          rk_codes[i,"CODE"],"','",
                          rk_codes[i,"PRODUCT_NC"],"','",
                          rk_codes[i,"FLOW"],"','",
                          rk_codes[i,"PERIOD"],"','",
                          rk_codes[i,"VALUE"],"','",
                          rk_codes[i,"QUANTITY"],"',",
                          ifelse(is.na(rk_codes[i,"SUP_QUANTITY"])==T,"null", paste0("'",rk_codes[i,"SUP_QUANTITY"],"'")),",",
#                          rk_codes[i,"SUP_QUANTITY"],",",
                          ifelse(is.na(rk_codes[i,"COUNTRY_FLAG"])==T,"null", paste0("'",rk_codes[i,"COUNTRY_FLAG"],"'"))
#                          rk_codes[i,"COUNTRY_FLAG"]
                          
                          ,")")
  )
}

dbDisconnect(conn)
detach(name = "package:RMySQL", unload=TRUE)
View(rk_codes[rk_codes$PRODUCT_NC=="8504210000" & 
              rk_codes$FLOW=="IM" &
              rk_codes$PERIOD==200807 &
              rk_codes$CODE=="RS"  
                  ,])
print(paste0("block ",month, " finished ", date()))

















rk_codes <- rkj[order(rkj$PERIOD, rkj$FLOW, rkj$PRODUCT_NC, rkj$CODE),] 


library(data.table)

dt1 <- data.table(rkt, key = "PARTNER") 
dt2 <- data.table(countries_rk, key = "PARTNER")
rk_ <- dt2[dt1]

library(sqldf)
rk_<-sqldf('select c.code, r.*
      from rkt r join countries_rk c on r.partner=c.partner
      ')

library(dplyr)
rk_<-right_join(x = rkt, y = countries_rk, by = "PARTNER")



sqldf('select CODE, count(*) from countries_rk group by CODE  having 
      count(*)>1
      ')
sqldf('
select CODE, product_nc, flow, period, count(*) 
from rk_codes
group by CODE, product_nc, flow, period
having count(*)>1
      ')
View(rk_codes[rk_codes$PRODUCT_NC=="9209997000" & 
                rk_codes$FLOW=="IM" &
                rk_codes$PERIOD==200901 &
                rk_codes$CODE=="DE"  
              ,])
