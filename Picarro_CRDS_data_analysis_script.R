getwd()
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
library(data.table)
source("Picarro_CRDS_data_cleaning_script.R")

####### Data importing and cleaning ########
#Picarro G2508 (2025-04-08)
CRDS.P8 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2025/04",
                   
                   output_path = "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean",
                   
                   result_file_name = "20250408-13_Ring_7.5_cycle_CRDS",
                   
                   gas = c("CO2", "CH4", "NH3", "H2O"),
                   
                   start_time = "2025-04-08 12:00:00",
                   
                   end_time = "2025-04-13 23:59:59",
                   
                   flush = 180, # Flush time in seconds
                   
                   interval = 450) # Total time at MPVPosition in seconds


# Read in the data
CRDS.P8 <- fread("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/20250408-13_Ring_7.5_cycle_CRDS.csv")

# Convert DATE.TIME to datetime format
CRDS.P8$DATE.TIME <- ymd_hms(CRDS.P8$DATE.TIME)

# Create an hourly timestamp to group by
CRDS.P8$hour <- floor_date(CRDS.P8$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
averages <- CRDS.P8 %>%
        group_by(hour, MPVPosition) %>%
        summarise(
                CO2_avg = mean(CO2, na.rm = TRUE),
                CH4_avg = mean(CH4, na.rm = TRUE),
                NH3_avg = mean(NH3, na.rm = TRUE),
                H2O_avg = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )

# Reshape to wide format, each gas and MPVPosition combination becomes a column
reshaped_CRDS.P8 <- averages %>%
        pivot_wider(
                names_from = MPVPosition,
                values_from = c(CO2_avg, CH4_avg, NH3_avg, H2O_avg),
                names_glue = "{.value}_MPV{MPVPosition}"
        )

# Rename columns as needed
reshaped_CRDS.P8 <- reshaped_CRDS.P8 %>%
        rename(
                CO2_in = CO2_avg_MPV2,
                CO2_N = CO2_avg_MPV1,
                CO2_S = CO2_avg_MPV3,
                CH4_in = CH4_avg_MPV2,
                CH4_N = CH4_avg_MPV1,
                CH4_S = CH4_avg_MPV3,
                NH3_in = NH3_avg_MPV2,
                NH3_N = NH3_avg_MPV1,
                NH3_S = NH3_avg_MPV3,
                H2O_in = H2O_avg_MPV2,
                H2O_N = H2O_avg_MPV1,
                H2O_S = H2O_avg_MPV3
        )

# Convert hour to datetime format
reshaped_CRDS.P8$hour <- ymd_hms(reshaped_CRDS.P8$hour)

# Write csv day wise
reshaped_CRDS_08_09 <- reshaped_CRDS.P8 %>% filter(hour >= ymd_hms("2025-04-08 12:00:00"), hour <= ymd_hms("2025-04-09 23:00:00"))
write.csv(reshaped_CRDS_08_09,"20250408-09_hourly_CRDS08.csv" , row.names = FALSE, quote = FALSE)


reshaped_CRDS_10_11 <- reshaped_CRDS.P8 %>% filter(hour >= ymd_hms("2025-04-10 00:00:00"), hour <= ymd_hms("2025-04-11 23:00:00"))
write.csv(reshaped_CRDS_10_11,"20250410-11_hourly_CRDS08.csv" , row.names = FALSE, quote = FALSE)


reshaped_CRDS_12_13 <- reshaped_CRDS.P8 %>% filter(hour >= ymd_hms("2025-04-12 00:00:00"), hour <= ymd_hms("2025-04-13 23:00:00"))
write.csv(reshaped_CRDS_12_13,"20250412-13_hourly_CRDS08.csv" , row.names = FALSE, quote = FALSE)

