getwd()
####### libraries ######
library(tidyverse)
library(reshape2)
library(hablar)
library(lubridate)
library(psych)
library(ggplot2)
library(readxl)
library(dplyr)
library(ggpubr)
library(readr)
library(scales)
source("remove_outliers_function.R")

####### Import Picarro dataset #######
CRDS8_20250925_20250930 <- read.csv("20250925.1206-20250930.2319_ATB_240avg_CRDS8.csv")
CRDS8_20250930_20251010 <- read.csv("20250930.2319-20251010.0923_ATB_240avg_CRDS8.csv")
CRDS8_20251010_20251024 <- read.csv("20251010.0923-20251024.1428_ATB_240avg_CRDS8.csv")
CRDS8_20251024_20251030 <- read.csv("20251024.1428-20251030.1506_ATB_240avg_CRDS8.csv")
CRDS8_20251030_20251101 <- read.csv("20251030.1506-20251101.0125_ATB_240avg_CRDS8.csv")
CRDS8_20251101_20251112 <- read.csv("20251101.0125-20251112.0155_ATB_240avg_CRDS8.csv")
CRDS8_20251112_20251114 <- read.csv("20251112.0155-20251114.0122_ATB_240avg_CRDS8.csv")
CRDS8_20251114_20251119 <- read.csv("20251114.0122-20251119.0640_ATB_240avg_CRDS8.csv")
CRDS8_20251209_20251231 <- read.csv("20251209.1200-20251231.2359_ATB_240avg_CRDS8.csv")


CRDS9_20250925_20250930 <- read.csv("20250925.1213-20250930.2330_ATB_240avg_CRDS9.csv")
CRDS9_20250930_20251010 <- read.csv("20250930.2330-20251010.0926_ATB_240avg_CRDS9.csv")
CRDS9_20251010_20251024 <- read.csv("20251010.0926-20251024.1430_ATB_240avg_CRDS9.csv")
CRDS9_20251024_20251030 <- read.csv("20251024.1430-20251030.1356_ATB_240avg_CRDS9.csv")
CRDS9_20251030_20251101 <- read.csv("20251030.1356-20251101.0125_ATB_240avg_CRDS9.csv")
CRDS9_20251101_20251112 <- read.csv("20251101.0125-20251112.0044_ATB_240avg_CRDS9.csv")
CRDS9_20251112_20251114 <- read.csv("20251112.0044-20251114.0122_ATB_240avg_CRDS9.csv")
CRDS9_20251114_20251119 <- read.csv("20251114.0122-20251119.0906_ATB_240avg_CRDS9.csv")


# Combine all into one data frame
CRDS_data <- bind_rows(
        CRDS8_20250925_20250930 %>% mutate(location = as.character(location)), 
        CRDS9_20250925_20250930 %>% mutate(location = as.character(location)),
        CRDS8_20250930_20251010 %>% mutate(location = as.character(location)),
        CRDS8_20251010_20251024 %>% mutate(location = as.character(location)),
        CRDS8_20251024_20251030 %>% mutate(location = as.character(location)),
        CRDS8_20251030_20251101 %>% mutate(location = as.character(location)),
        CRDS8_20251101_20251112 %>% mutate(location = as.character(location)),
        CRDS8_20251112_20251114 %>% mutate(location = as.character(location)),
        CRDS8_20251114_20251119 %>% mutate(location = as.character(location)),
        CRDS9_20250930_20251010 %>% mutate(location = as.character(location)),
        CRDS9_20251010_20251024 %>% mutate(location = as.character(location)),
        CRDS9_20251024_20251030 %>% mutate(location = as.character(location)),
        CRDS9_20251030_20251101 %>% mutate(location = as.character(location)),
        CRDS9_20251101_20251112 %>% mutate(location = as.character(location)),
        CRDS9_20251112_20251114 %>% mutate(location = as.character(location)),
        CRDS9_20251114_20251119 %>% mutate(location = as.character(location)),
        CRDS8_20251209_20251231 %>% mutate(location = as.character(location))
) %>%
        mutate(
                DATE.HOUR = floor_date(ymd_hms(DATE.TIME, tz = "Europe/Berlin"), unit = "hour"),
                location = as.factor(location),
                analyzer = as.factor(analyzer)
        ) %>%
        group_by(DATE.HOUR, location, analyzer) %>%
        summarise(
                CO2_raw = mean(CO2, na.rm = TRUE),
                NH3_raw = mean(NH3, na.rm = TRUE),
                .groups = "drop"
        )

write.csv(CRDS_data, "CRDS8+9_high_res_20250925_20251231.csv", row.names = FALSE)
        
####### Import OTICE Dataset #######
OTICE_20250929_20251126 <- read.csv("D:/Data Analysis/Gas_data/Raw_data/OTICE_raw/2025/average_hourly_data_OTICE_20250929_20251126NEWCALIBRATION.csv")
OTICE_20251126_20251231 <- read.csv("D:/Data Analysis/Gas_data/Raw_data/OTICE_raw/2025/average_hourly_data_OTICE_20251126_20251231NEWCALIBRATION.csv")

# Combine all into one dataframe
OTICE_data <- bind_rows(
        OTICE_20250929_20251126,
        OTICE_20251126_20251231) %>%
        mutate(Datetime_Berlin = ifelse(
                nchar(Datetime_Berlin) == 10,
                paste0(Datetime_Berlin, " 00:00:00"),
                Datetime_Berlin),
               DATE.HOUR = ymd_hms(Datetime_Berlin, tz = "Europe/Berlin"),
                analyzer = as.factor(Type),
                location = as.factor(Node)) %>%
        rename(CO2_raw  = CO2.AVG_hourly,
               CO2_corr = CO2.AVG_barn_hourly,
               NH3_raw  = NH3_ppm_hourly,
               NH3_corr = NH3_ppm_barn_hourly) %>%
        select(-Datetime_Berlin, -Type, -Node)

write.csv(OTICE_data, "OTICE_high_res_20250930_20251231.csv", row.names = FALSE)
                
###### Comparison ######
library(dplyr)
library(stringr)
library(lubridate)
library(readr)
library(readxl)
library(purrr)
library(ggplot2)

#-----------------------------
# Read files
#-----------------------------
otice <- read_csv("OTICE_high_res_20250930_20251231.csv", show_col_types = FALSE)
crds  <- read_csv("CRDS8+9_high_res_20250925_20251231.csv", show_col_types = FALSE)
meta  <- read_excel("OTICE_meta.xlsx")

#-----------------------------
# Clean OTICE
#-----------------------------
otice2 <- otice %>%
        mutate(
                DATE.HOUR = as.character(DATE.HOUR),
                DATE.HOUR = if_else(
                        str_detect(DATE.HOUR, "^\\d{4}-\\d{2}-\\d{2}$"),
                        paste0(DATE.HOUR, " 00:00:00"),
                        DATE.HOUR
                ),
                DATE.HOUR = ymd_hms(DATE.HOUR, tz = "UTC"),
                OTICE_location = as.integer(str_remove(location, "^O"))
        ) %>%
        select(DATE.HOUR, analyzer, location, OTICE_location, CO2_raw, NH3_raw, CO2_corr, NH3_corr)

#-----------------------------
# Clean CRDS
#-----------------------------
crds2 <- crds %>%
        mutate(
                DATE.HOUR = as.character(DATE.HOUR),
                DATE.HOUR = if_else(
                        str_detect(DATE.HOUR, "^\\d{4}-\\d{2}-\\d{2}$"),
                        paste0(DATE.HOUR, " 00:00:00"),
                        DATE.HOUR
                ),
                DATE.HOUR = ymd_hms(DATE.HOUR, tz = "UTC"),
                CRDS_location = as.integer(str_extract(as.character(location), "\\d+")),
                CRDS_analyzer = as.character(analyzer)
        ) %>%
        select(DATE.HOUR, analyzer, CRDS_analyzer, location, CRDS_location, CO2_raw, NH3_raw)

#-----------------------------
# Clean metadata
#-----------------------------
meta2 <- meta %>%
        mutate(
                DATE.HOUR_start = as.character(DATE.HOUR_start),
                DATE.HOUR_end   = as.character(DATE.HOUR_end),
                
                DATE.HOUR_start = if_else(
                        str_detect(DATE.HOUR_start, "^\\d{4}-\\d{2}-\\d{2}$"),
                        paste0(DATE.HOUR_start, " 00:00:00"),
                        DATE.HOUR_start
                ),
                DATE.HOUR_end = if_else(
                        str_detect(DATE.HOUR_end, "^\\d{4}-\\d{2}-\\d{2}$"),
                        paste0(DATE.HOUR_end, " 23:59:59"),
                        DATE.HOUR_end
                ),
                
                DATE.HOUR_start = ymd_hms(DATE.HOUR_start, tz = "UTC"),
                DATE.HOUR_end   = ymd_hms(DATE.HOUR_end, tz = "UTC"),
                
                OTICE_location = as.integer(OTICE_location),
                CRDS_location  = as.integer(CRDS_location),
                CRDS_analyzer  = as.character(CRDS_analyzer)
        )

make_period_data <- function(i, meta_tbl, otice_tbl, crds_tbl) {
        
        m <- meta_tbl[i, ]
        
        ot_sub <- otice_tbl %>%
                filter(
                        OTICE_location == m$OTICE_location,
                        DATE.HOUR >= m$DATE.HOUR_start,
                        DATE.HOUR <= m$DATE.HOUR_end
                ) %>%
                arrange(DATE.HOUR)
        
        cr_sub <- crds_tbl %>%
                filter(
                        CRDS_location == m$CRDS_location,
                        CRDS_analyzer == m$CRDS_analyzer,
                        DATE.HOUR >= m$DATE.HOUR_start,
                        DATE.HOUR <= m$DATE.HOUR_end
                ) %>%
                arrange(DATE.HOUR)
        
        list(
                meta = m,
                otice = ot_sub,
                crds = cr_sub
        )
}

period_list <- map(seq_len(nrow(meta2)), make_period_data,
                   meta_tbl = meta2, otice_tbl = otice2, crds_tbl = crds2)

period_summary <- map_dfr(seq_along(period_list), function(i) {
        
        x <- period_list[[i]]
        m <- x$meta
        
        tibble(
                period_index    = i,
                OTICE_location  = m$OTICE_location,
                CRDS_location   = m$CRDS_location,
                CRDS_analyzer   = m$CRDS_analyzer,
                start           = m$DATE.HOUR_start,
                end             = m$DATE.HOUR_end,
                otice_n         = nrow(x$otice),
                crds_n          = nrow(x$crds),
                co2_otice_n     = sum(!is.na(x$otice$CO2_raw)),
                co2_crds_n      = sum(!is.na(x$crds$CO2_raw)),
                nh3_otice_n     = sum(!is.na(x$otice$NH3_raw)),
                nh3_crds_n      = sum(!is.na(x$crds$NH3_raw))
        )
})

plot_period_ts <- function(period_obj, gas = c("CO2_raw", "NH3_raw")) {
        
        gas <- match.arg(gas)
        
        m   <- period_obj$meta
        ot  <- period_obj$otice
        cr  <- period_obj$crds
        
        ot_plot <- ot %>%
                transmute(
                        DATE.HOUR = DATE.HOUR,
                        source = "OTICE",
                        value = .data[[gas]]
                )
        
        cr_plot <- cr %>%
                transmute(
                        DATE.HOUR = DATE.HOUR,
                        source = "CRDS",
                        value = .data[[gas]]
                )
        
        plot_df <- bind_rows(ot_plot, cr_plot)
        
        ggplot(plot_df, aes(x = DATE.HOUR, y = value, color = source)) +
                geom_line(na.rm = FALSE) +
                theme_bw() +
                labs(
                        title = paste0(
                                gas, " | OTICE O", sprintf("%02d", m$OTICE_location),
                                " vs ", m$CRDS_analyzer, " L", m$CRDS_location
                        ),
                        subtitle = paste0(
                                format(m$DATE.HOUR_start, "%Y-%m-%d %H:%M:%S"),
                                " to ",
                                format(m$DATE.HOUR_end, "%Y-%m-%d %H:%M:%S")
                        ),
                        x = "Date-Time",
                        y = gas,
                        color = NULL
                )
}

plot_period_ts(period_list[[1]], gas = "CO2_raw")
plot_period_ts(period_list[[1]], gas = "NH3_raw")
