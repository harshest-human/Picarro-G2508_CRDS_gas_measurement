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
CRDS9_20250930_20251010 <- read.csv("20250930.2330-20251010.0926_ATB_240avg_CRDS9.csv")
CRDS9_20251010_20251024 <- read.csv("20251010.0926-20251024.1430_ATB_240avg_CRDS9.csv")
CRDS9_20251024_20251030 <- read.csv("20251024.1430-20251030.1356_ATB_240avg_CRDS9.csv")
CRDS9_20251030_20251101 <- read.csv("20251030.1356-20251101.0125_ATB_240avg_CRDS9.csv")
CRDS9_20251101_20251112 <- read.csv("20251101.0125-20251112.0044_ATB_240avg_CRDS9.csv")
CRDS9_20251112_20251114 <- read.csv("20251112.0044-20251114.0122_ATB_240avg_CRDS9.csv")
CRDS9_20251114_20251119 <- read.csv("20251114.0122-20251119.0906_ATB_240avg_CRDS9.csv")
CRDS8_20251209_20251231 <- read.csv("20251209.1200-20251231.2359_ATB_240avg_CRDS8.csv")

# Combine all into one data frame
CRDS_data <- bind_rows(
        CRDS9_20250930_20251010,
        CRDS9_20251010_20251024,
        CRDS9_20251024_20251030,
        CRDS9_20251030_20251101,
        CRDS9_20251101_20251112,
        CRDS9_20251112_20251114,
        CRDS9_20251114_20251119,
        CRDS8_20251209_20251231) %>%
        mutate(DATE.HOUR = floor_date(ymd_hms(DATE.TIME, tz = "Europe/Berlin"), unit = "hour"),
               location = as.factor(location),
               analyzer = as.factor(analyzer)) %>%
        group_by(DATE.HOUR, location, analyzer) %>%
        summarise(CO2_raw = mean(CO2, na.rm = TRUE),
                  NH3_raw = mean(NH3, na.rm = TRUE),
                  .groups = "drop") 
                
        
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
                

ggplot() +
        geom_line(data = OTICE_data,
                  aes(DATE.HOUR, CO2_raw,
                      color = location,
                      linetype = analyzer)) +
        geom_line(data = CRDS_data,
                  aes(DATE.HOUR, CO2_raw,
                      color = location,
                      linetype = analyzer)) +
        theme_minimal()

