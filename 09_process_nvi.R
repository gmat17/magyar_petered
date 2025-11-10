setwd("~/Downloads/egyetem/TDK/magyar_petered_main/magyar_petered/data")

# ---- Read-in necessary files ----
# nvi
params <- readxl::read_xls('ep2024_munis_result.xls',sheet = 'Paraméterek')

counties <- c("baranya","bacs_kisk","bekes","baz","cscs","fejer","gyorms",
       "hajdub","heves","jasznk","komesz","nograd","pest","somogy","szabolcs",
       "tolna","vas","veszprem","zala")
nvi <- data.frame()

for (i in counties){
  county <- readxl::read_xls('ep2024_munis_result.xls',sheet = i)
  nvi <- rbind(nvi, county)
}

colnames(nvi)[5:21] <- params$sign_in_code
sum(nvi$no_stamp)