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

####### ATB Data importing and cleaning ########
#Picarro G2508
ATB_CRDS.P8 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2025/04",
                   
                   output_path = "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean",
                   
                   result_file_name = "20250408-15_Ring_7.5_cycle_ATB_CRDS.P8",
                   
                   gas = c("CO2", "CH4", "NH3", "H2O"),
                   
                   start_time = "2025-04-08 12:00:00",
                   
                   end_time = "2025-04-15 12:59:59",
                   
                   flush = 180, # Flush time in seconds
                   
                   interval = 450) # Total time at MPVPosition in seconds


# Read in the data
ATB_CRDS.P8 <- fread("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/20250408-15_Ring_7.5_cycle_ATB_CRDS.P8.csv")

# Convert DATE.TIME to datetime format
ATB_CRDS.P8$DATE.TIME <- ymd_hms(ATB_CRDS.P8$DATE.TIME)

# Create an hourly timestamp to group by
ATB_CRDS.P8$hour <- floor_date(ATB_CRDS.P8$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
ATB_averages <- ATB_CRDS.P8 %>%
        group_by(hour, MPVPosition) %>%
        summarise(
                CO2_avg = mean(CO2, na.rm = TRUE),
                CH4_avg = mean(CH4, na.rm = TRUE),
                NH3_avg = mean(NH3, na.rm = TRUE),
                H2O_avg = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )

# Reshape to wide format, each gas and MPVPosition combination becomes a column
reshaped_ATB_CRDS.P8 <- ATB_averages %>%
        filter(MPVPosition %in% c(1,2,3)) %>%
        pivot_wider(
                names_from = MPVPosition,
                values_from = c(CO2_avg, CH4_avg, NH3_avg, H2O_avg),
                names_glue = "{.value}_MPV{MPVPosition}"
        )

# Rename columns as needed
reshaped_ATB_CRDS.P8 <- reshaped_ATB_CRDS.P8 %>%
        rename(
                ATB_CO2_in = CO2_avg_MPV2,
                ATB_CO2_N = CO2_avg_MPV1,
                ATB_CO2_S = CO2_avg_MPV3,
                ATB_CH4_in = CH4_avg_MPV2,
                ATB_CH4_N = CH4_avg_MPV1,
                ATB_CH4_S = CH4_avg_MPV3,
                ATB_NH3_in = NH3_avg_MPV2,
                ATB_NH3_N = NH3_avg_MPV1,
                ATB_NH3_S = NH3_avg_MPV3,
                ATB_H2O_in = H2O_avg_MPV2,
                ATB_H2O_N = H2O_avg_MPV1,
                ATB_H2O_S = H2O_avg_MPV3
        )

# Convert hour to datetime format
reshaped_ATB_CRDS.P8$hour <- ymd_hms(reshaped_ATB_CRDS.P8$hour)

# Write csv day wise
reshaped_ATB_CRDS.P8 <- reshaped_ATB_CRDS.P8 %>% filter(hour >= ymd_hms("2025-04-08 12:00:00"), hour <= ymd_hms("2025-04-15 12:59:59"))
write.csv(reshaped_ATB_CRDS.P8,"20250408-15_hourly_ATB_CRDS.P8.csv" , row.names = FALSE, quote = FALSE)


####### UB Data importing and cleaning ########
#Picarro G2508
UB_CRDS.P8 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/Ringversuche_2025_raw/UB_CRDS_raw",
                      
                      output_path = "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean",
                      
                      result_file_name = "20250408-15_Ring_7.5_cycle_UB_CRDS.P8",
                      
                      gas = c("CO2", "CH4", "NH3", "H2O"),
                      
                      start_time = "2025-04-08 12:00:00",
                      
                      end_time = "2025-04-15 12:59:59",
                      
                      flush = 180, # Flush time in seconds
                      
                      interval = 450) # Total time at MPVPosition in seconds


# Read in the data
UB_CRDS.P8 <- fread("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/20250408-15_Ring_7.5_cycle_UB_CRDS.P8.csv")

# Convert DATE.TIME to datetime format
UB_CRDS.P8$DATE.TIME <- ymd_hms(UB_CRDS.P8$DATE.TIME)

# Create an hourly timestamp to group by
UB_CRDS.P8$hour <- floor_date(UB_CRDS.P8$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
UB_averages <- UB_CRDS.P8 %>%
        group_by(hour, MPVPosition) %>%
        summarise(
                CO2_avg = mean(CO2, na.rm = TRUE),
                CH4_avg = mean(CH4, na.rm = TRUE),
                NH3_avg = mean(NH3, na.rm = TRUE),
                H2O_avg = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )

# Reshape to wide format, each gas and MPVPosition combination becomes a column
reshaped_UB_CRDS.P8 <- UB_averages %>%
        filter(MPVPosition %in% c(1,8,9)) %>%
        pivot_wider(
                names_from = MPVPosition,
                values_from = c(CO2_avg, CH4_avg, NH3_avg, H2O_avg),
                names_glue = "{.value}_MPV{MPVPosition}"
        )

# Rename columns as needed
reshaped_UB_CRDS.P8 <- reshaped_UB_CRDS.P8 %>%
        rename(
                UB_CO2_in = CO2_avg_MPV1,
                UB_CO2_N = CO2_avg_MPV8,
                UB_CO2_S = CO2_avg_MPV9,
                UB_CH4_in = CH4_avg_MPV1,
                UB_CH4_N = CH4_avg_MPV8,
                UB_CH4_S = CH4_avg_MPV9,
                UB_NH3_in = NH3_avg_MPV1,
                UB_NH3_N = NH3_avg_MPV8,
                UB_NH3_S = NH3_avg_MPV9,
                UB_H2O_in = H2O_avg_MPV1,
                UB_H2O_N = H2O_avg_MPV8,
                UB_H2O_S = H2O_avg_MPV9
        )

# Convert hour to datetime format
reshaped_UB_CRDS.P8$hour <- ymd_hms(reshaped_UB_CRDS.P8$hour)

# Write csv day wise
reshaped_UB_CRDS.P8 <- reshaped_UB_CRDS.P8 %>% filter(hour >= ymd_hms("2025-04-08 12:00:00"), hour <= ymd_hms("2025-04-15 12:59:59"))
write.csv(reshaped_UB_CRDS.P8,"20250408-09_hourly_UB_CRDS.P8.csv" , row.names = FALSE, quote = FALSE)



####### UB Data importing and cleaning ########
#Picarro G2508
LUFA_CRDS.P8 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/Ringversuche_2025_raw/LUFA_CRDS_raw",
                        
                        output_path = "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean",
                        
                        result_file_name = "20250408-15_Ring_7.5_cycle_LUFA_CRDS.P8",
                        
                        gas = c("CO2", "CH4", "NH3", "H2O"),
                        
                        start_time = "2025-04-08 12:00:00",
                        
                        end_time = "2025-04-15 12:59:59",
                        
                        flush = 180, # Flush time in seconds
                        
                        interval = 450) # Total time at MPVPosition in seconds


# Read in the data
LUFA_CRDS.P8 <- fread("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/20250408-15_Ring_7.5_cycle_LUFA_CRDS.P8.csv")

# Convert DATE.TIME to datetime format
LUFA_CRDS.P8$DATE.TIME <- ymd_hms(LUFA_CRDS.P8$DATE.TIME)

# Create an hourly timestamp to group by
LUFA_CRDS.P8$hour <- floor_date(LUFA_CRDS.P8$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
LUFA_averages <- LUFA_CRDS.P8 %>%
        group_by(hour, MPVPosition) %>%
        summarise(
                CO2_avg = mean(CO2, na.rm = TRUE),
                CH4_avg = mean(CH4, na.rm = TRUE),
                NH3_avg = mean(NH3, na.rm = TRUE),
                H2O_avg = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )

# Reshape to wide format, each gas and MPVPosition combination becomes a column
reshaped_LUFA_CRDS.P8 <- LUFA_averages %>%
        filter(MPVPosition %in% c(1,3,2)) %>%
        pivot_wider(
                names_from = MPVPosition,
                values_from = c(CO2_avg, CH4_avg, NH3_avg, H2O_avg),
                names_glue = "{.value}_MPV{MPVPosition}"
        )

# Rename columns as needed
reshaped_LUFA_CRDS.P8 <- reshaped_LUFA_CRDS.P8 %>%
        rename(
                LUFA_CO2_in = CO2_avg_MPV1,
                LUFA_CO2_N = CO2_avg_MPV3,
                LUFA_CO2_S = CO2_avg_MPV2,
                LUFA_CH4_in = CH4_avg_MPV1,
                LUFA_CH4_N = CH4_avg_MPV3,
                LUFA_CH4_S = CH4_avg_MPV2,
                LUFA_NH3_in = NH3_avg_MPV1,
                LUFA_NH3_N = NH3_avg_MPV3,
                LUFA_NH3_S = NH3_avg_MPV2,
                LUFA_H2O_in = H2O_avg_MPV1,
                LUFA_H2O_N = H2O_avg_MPV3,
                LUFA_H2O_S = H2O_avg_MPV2
        )

# Convert hour to datetime format
reshaped_LUFA_CRDS.P8$hour <- ymd_hms(reshaped_LUFA_CRDS.P8$hour)

# Write csv day wise
reshaped_LUFA_CRDS.P8 <- reshaped_LUFA_CRDS.P8 %>% filter(hour >= ymd_hms("2025-04-08 12:00:00"), hour <= ymd_hms("2025-04-15 12:59:59"))
write.csv(reshaped_LUFA_CRDS.P8,"20250408-09_hourly_LUFA_CRDS.P8.csv" , row.names = FALSE, quote = FALSE)
