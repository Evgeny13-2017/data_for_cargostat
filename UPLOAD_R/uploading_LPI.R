library(readxl)
library(RMySQL)
conn = dbConnect(MySQL(), 
                 user='cb74929_cargo',
                 password='selmed45', 
                 dbname='cb74929_cargo', 
                 host='92.53.96.170')
countries<-dbGetQuery(conn, "select ALFA2, ALFA3, NAME_EN from cb74929_cargo.COUNTRIES where alfa2 is not null")
dbDisconnect(conn)
detach(name = "package:RMySQL", unload=TRUE)

lpi_nar<- data.frame(YEAR = character(),
                     COUNTRY = character(),
                     INDICATOR = character(),
                     SCORE = numeric(),
                     RANK = integer(),
                     stringsAsFactors = F)

download.file(url="http://lpi.worldbank.org/sites/default/files/International_LPI_from_2007_to_2016.xlsx", destfile = "C:/R/DoB/LPI.xlsx", mode = "wb")
LPI<-read_excel(path = "C:/R/DoB/LPI.xlsx", sheet = "2016")
year<- '2016'
LPI<-LPI[is.na(LPI[,1])==F,]
LPI<-LPI[-1,]
lpi_nar <- rbind(lpi_nar,
                 data.frame(YEAR = year,
                           COUNTRY = LPI[,2],
                            INDICATOR = 'Overall',
                            SCORE = LPI[,3],
                            RANK = LPI[,6],
                            stringsAsFactors = F)
                  )

lpi_nar <- rbind(lpi_nar,
                 data.frame(YEAR = year,
                            COUNTRY = LPI[,2],
                            INDICATOR = 'Customs',
                            SCORE = LPI[,10],
                            RANK = LPI[,11],
                            stringsAsFactors = F)
                 )

lpi_nar <- rbind(lpi_nar,
                 data.frame(YEAR = year,
                            COUNTRY = LPI[,2],
                            INDICATOR = 'Infrastructure',
                            SCORE = LPI[,12],
                            RANK = LPI[,13],
                            stringsAsFactors = F)
)

lpi_nar <- rbind(lpi_nar,
                 data.frame(YEAR = year,
                            COUNTRY = LPI[,2],
                            INDICATOR = 'International shipments',
                            SCORE = LPI[,14],
                            RANK = LPI[,15],
                            stringsAsFactors = F)
)

lpi_nar <- rbind(lpi_nar,
                 data.frame(YEAR = year,
                            COUNTRY = LPI[,2],
                            INDICATOR = 'Logistics competence',
                            SCORE = LPI[,16],
                            RANK = LPI[,17],
                            stringsAsFactors = F)
)

lpi_nar <- rbind(lpi_nar,
                 data.frame(YEAR = year,
                            COUNTRY = LPI[,2],
                            INDICATOR = 'Tracking & tracing',
                            SCORE = LPI[,18],
                            RANK = LPI[,19],
                            stringsAsFactors = F)
)

lpi_nar <- rbind(lpi_nar,
                 data.frame(YEAR = year,
                            COUNTRY = LPI[,2],
                            INDICATOR = 'Timeliness',
                            SCORE = LPI[,20],
                            RANK = LPI[,21],
                            stringsAsFactors = F)
)






colnames(lpi_nar)[2] <- 'ALFA3'
lpi_nar_ <- merge(x = lpi_nar, y = countries, by = "ALFA3")


library(RMySQL)
conn = dbConnect(MySQL(), 
                 user='cb74929_cargo',
                 password='selmed45', 
                 dbname='cb74929_cargo', 
                 host='92.53.96.170')
dbWriteTable(conn = conn, name = "LPI", value = lpi_nar, overwrite = F, append = T, row.names = F)
dbDisconnect(conn)
detach(name = "package:RMySQL", unload=TRUE)
