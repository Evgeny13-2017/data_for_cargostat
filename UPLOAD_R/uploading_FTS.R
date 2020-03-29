#----------------------------------------------------#
#---------- Upload FTS import/export data -----------#
#----------------------------------------------------#

setwd("C:/R/FTS")
library(RMySQL)
library(foreign)
fts <- read.dbf("201703.dbf", as.is = T)
fts$NAPR <- iconv(fts$NAPR, from="CP866", to="CP1251")
fts$EDIZM <- iconv(fts$EDIZM, from="CP866", to="CP1251")
fts$REGION <- iconv(fts$REGION, from="CP866", to="CP1251")
fts$REGION_S <- iconv(fts$REGION_S, from="CP866", to="CP1251")
dir <- data.frame(NAPR = c("ИМ","ЭК"),
                  FLOW = c("IM","EC"),
                  stringsAsFactors = F)
fts <- merge(x = fts, y = dir, by = "NAPR")
fts <- data.frame(FLOW = fts$FLOW,
                  PERIOD = as.character(paste0(substr(fts$PERIOD,4,7),substr(fts$PERIOD,1,2))),
                  REGION = as.character(substr(fts$REGION,1,5)),
                  REGION_S = as.character(substr(fts$REGION_S,1,2)),
                  PRODUCT_NC = as.character(fts$TNVED),
                  ALFA2 = as.character(fts$STRANA),
                  STOIM = as.numeric(fts$STOIM),
                  NETTO = as.numeric(fts$NETTO),
                  KOL = as.numeric(fts$KOL),
                  stringsAsFactors = F
                  )
conn = dbConnect(MySQL(), 
                 user='cb74929_cargo',
                 password='selmed45', 
                 dbname='cb74929_cargo', 
                 host='92.53.96.170')
dbWriteTable(conn, "FTS", fts, overwrite = F, append = T, row.names = F)
dbDisconnect(conn)


#----------------------------------------------------#
#---------- Test queries for import/export ----------#
#----------------------------------------------------#
dbGetQuery(conn, "select period, count(*) from cb74929_cargo.FTS group by period")
dbGetQuery(conn, "
select period, sum(stoim)/sum(netto), sum(netto) from
cb74929_cargo.FTS
where 1=1
and product_nc = '3919900000'
and alfa2 = 'CN'
and flow = 'IM'
group by period
           ")

#----------------------------------------------------#
#--------------- Uploading NSI data -----------------#
#----------------------------------------------------#

#--------------- Uploading TNVED data ---------------#
library(foreign)
library(RMySQL)
fts_tnved <- read.dbf("TNVED.dbf", as.is = T)
fts_tnved$KOD <- iconv(fts_tnved$KOD, from="CP866", to="CP1251")
fts_tnved$SIMPLE_NAM <- iconv(fts_tnved$SIMPLE_NAM, from="CP866", to="CP1251")
fts_tnved$KOD <- iconv(fts_tnved$KOD, from="CP1251", to="UTF-8")
fts_tnved$SIMPLE_NAM <- iconv(fts_tnved$SIMPLE_NAM, from="CP1251", to="UTF-8")
colnames(fts_tnved)<-c("PRODUCT_NC","DESCR")
write.table(fts_tnved,file="fts_tnved.txt", fileEncoding ="utf8")
fts_tnved_enc <- read.table(file="fts_tnved.txt",encoding="utf8")
conn = dbConnect(MySQL(), 
                 user='cb74929_cargo',
                 password='selmed45', 
                 dbname='cb74929_cargo', 
                 host='92.53.96.170')
dbWriteTable(conn, "FTS_TNVED", fts_tnved_enc, overwrite = T, row.names = F)
dbDisconnect(conn)

#--------------- Uploading region data ---------------#
fts_region <- read.dbf("FO.dbf", as.is = T)
fts_region$OKATO_1 <- iconv(fts_region$OKATO_1, from="CP866", to="CP1251")
fts_region$OKATO_1_N <- iconv(fts_region$OKATO_1_N, from="CP866", to="CP1251")
fts_region$OKATO_1 <- iconv(fts_region$OKATO_1, from="CP1251", to="UTF-8")
fts_region$OKATO_1_N <- iconv(fts_region$OKATO_1_N, from="CP1251", to="UTF-8")
colnames(fts_region)<-c("OKATO","OKATO_N")
write.table(fts_region,file="fts_region.txt", fileEncoding ="utf8")
fts_region_enc <- read.table(file="fts_region.txt",encoding="utf8")
conn = dbConnect(MySQL(), 
                 user='cb74929_cargo',
                 password='selmed45', 
                 dbname='cb74929_cargo', 
                 host='92.53.96.170')
dbWriteTable(conn, "FTS_REGION", fts_region_enc, overwrite = T, row.names = F)
dbDisconnect(conn)

#------------ Uploading subregion data ---------------#
fts_subrf <- read.dbf("SUBRF.dbf", as.is = T)
fts_subrf$OKATO_2 <- iconv(fts_subrf$OKATO_2, from="CP866", to="CP1251")
fts_subrf$OKATO_2_N <- iconv(fts_subrf$OKATO_2_N, from="CP866", to="CP1251")
fts_subrf$OKATO_2 <- iconv(fts_subrf$OKATO_2, from="CP1251", to="UTF-8")
fts_subrf$OKATO_2_N <- iconv(fts_subrf$OKATO_2_N, from="CP1251", to="UTF-8")
colnames(fts_subrf)<-c("OKATO","OKATO_N")
write.table(fts_subrf,file="fts_subrf.txt", fileEncoding ="utf8")
fts_subrf_enc <- read.table(file="fts_subrf.txt",encoding="utf8")
conn = dbConnect(MySQL(), 
                 user='cb74929_cargo',
                 password='selmed45', 
                 dbname='cb74929_cargo', 
                 host='92.53.96.170')
dbWriteTable(conn, "FTS_SUBRF", fts_subrf_enc, overwrite = T, row.names = F)
dbDisconnect(conn)

#------------ Uploading UOM data ---------------------#
uom <- read.dbf("ED.dbf", as.is = T)
uom$KOD <- iconv($NAME, from="CP866", to="CP1251")
oksm$KOD <- iconv(oksm$KOD, from="CP866", to="CP1251")


#------------ Uploading subregion data ---------------#
oksm <- read.dbf("OKSM.dbf", as.is = T)
oksm$NAME <- iconv(oksm$NAME, from="CP866", to="CP1251")
oksm$KOD <- iconv(oksm$KOD, from="CP866", to="CP1251")

library(RMySQL)
conn = dbConnect(MySQL(), 
                 user='cb74929_cargo',
                 password='selmed45', 
                 dbname='cb74929_cargo', 
                 host='92.53.96.170')
dbGetQuery(conn, "update cb74929_cargo.FTS c
           set c.FLOW = 'EX' where c.FLOW != 'IM'
           ")
