#----------------------------------------------------#
#---------- Update dashboard for main page ----------#
#----------------------------------------------------#

setwd("C:/R/FTS")
library(RMySQL)
conn = dbConnect(MySQL(), 
                 user='cb74929_cargo',
                 password='selmed45', 
                 dbname='cb74929_cargo', 
                 host='92.53.96.170')
max_period <- dbGetQuery(conn, 'select max(period) from cb74929_cargo.FTS')
max_period <- as.numeric(max_period[1,1])
rep_start  <- as.numeric(paste0(substr(max_period,1,4),'01'))
rep_end    <- max_period
ref_start  <- rep_start - 100
ref_end    <- rep_end - 100
sql_dash   <- paste0('
select hd.high_group HIGH_GROUP, hd.high_group_descr HIGH_GROUP_DESCR, f.flow FLOW,
sum(case when f.period between ', ref_start, ' and ', ref_end, ' then f.stoim end) REF_VAL,
sum(case when f.period between ', rep_start, ' and ', rep_end, ' then f.stoim end) REP_VAL,
sum(case when f.period between ', ref_start, ' and ', ref_end, ' then f.netto end) REF_NET,
sum(case when f.period between ', rep_start, ' and ', rep_end, ' then f.netto end) REP_NET
from cb74929_cargo.FTS f join cb74929_cargo.HIGH_GROUP h 
on substr(f.product_nc,1,2) = h.nc_group
join cb74929_cargo.HIGH_GROUP_DESCR hd
on hd.high_group = h.high_group
where
f.period between ', ref_start, ' and ', ref_end, '
or 
f.period between ', rep_start, ' and ', rep_end, '
group by hd.high_group, hd.high_group_descr, f.flow')
dashboard <- dbGetQuery(conn = conn, statement = sql_dash)
dbDisconnect(conn)
detach(name = "package:RMySQL", unload=TRUE)

library(sqldf)
mon <- data.frame(MM  = c('01','02','03','04','05','06','07','08','09','10','11','12'),
                  MMM = c('янв','фев','мар','апр','май','июн','июл','авг','сен','окт','нбр','дек'),
                  stringsAsFactors = F)
periods <- data.frame(
  REF_PERIOD = paste0(
    merge(x = data.frame(MM = substr(ref_start,5,6), stringsAsFactors = F),
          y = mon,
          by = "MM")[1,"MMM"],
    "-",
    merge(x = data.frame(MM = substr(ref_end,5,6), stringsAsFactors = F),
          y = mon,
          by = "MM")[1,"MMM"],
    " ",
    substr(ref_start,3,4)),
  REP_PERIOD = paste0(
    merge(x = data.frame(MM = substr(rep_start,5,6), stringsAsFactors = F),
          y = mon,
          by = "MM")[1,"MMM"],
    "-",
    merge(x = data.frame(MM = substr(rep_end,5,6), stringsAsFactors = F),
          y = mon,
          by = "MM")[1,"MMM"],
    " ",
    substr(rep_start,3,4)),
  stringsAsFactors = F)

dash <- sqldf('
with tot as (
select flow, 
sum(ref_val) ref_val, sum(rep_val) rep_val, 
sum(ref_net) ref_net, sum(rep_net) rep_net
from dashboard
group by flow)

select d.high_group, d.high_group_descr, p.REF_PERIOD, p.REP_PERIOD, d.flow, 
d.ref_val, d.rep_val, 
case when d.rep_val > d.ref_val then 100 * (d.rep_val - d.ref_val) / d.ref_val else 100 * (d.ref_val - d.rep_val) / d.ref_val end DLT_VAL,
case when d.rep_val > d.ref_val then "+" else "-" end SGN_VAL,
100*d.ref_val/t.ref_val REF_VAL_SHARE,
100*d.rep_val/t.rep_val REP_VAL_SHARE,
d.ref_net, d.rep_net,
case when d.rep_net > d.ref_net then 100 * (d.rep_net - d.ref_net) / d.ref_net else 100 * (d.ref_net - d.rep_net) / d.ref_net end DLT_NET,
case when d.rep_net > d.ref_net then "+" else "-" end SGN_NET,
100*d.ref_net/t.ref_net REF_NET_SHARE,
100*d.rep_net/t.rep_net REP_NET_SHARE

from dashboard d join tot t on t.flow = d.flow
join periods p on 1 = 1
order by d.flow, 100*d.rep_val/t.rep_val desc
')
dash$HIGH_GROUP_DESCR <- iconv(dash$HIGH_GROUP_DESCR, from="UTF-8", to="CP1251")

write.table(dash,file="fts_dash.txt", fileEncoding ="utf8")
dash_enc <- read.table(file="fts_dash.txt",encoding="utf8")
# dash$HIGH_GROUP_DESCR <- 123
# dash$REF_PERIOD<-123
# dash$REP_PERIOD<-123
library(RMySQL)
conn = dbConnect(MySQL(), 
                 user='cb74929_cargo',
                 password='selmed45', 
                 dbname='cb74929_cargo', 
                 host='92.53.96.170')
dbGetQuery(conn, 'delete from cb74929_cargo.FTS_DASHBOARD')
dbWriteTable(conn, "FTS_DASHBOARD", dash_enc, overwrite = F, append = T, row.names = F)
dbDisconnect(conn)