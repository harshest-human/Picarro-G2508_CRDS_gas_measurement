##### libraries #######
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
ATB_CRDS.1 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/Ringversuche_2025_raw/ATB_CRDS_raw/2025",
                      
                   gas = c("CO2", "CH4", "NH3", "H2O"),
                   
                   start_time = "2025-04-08 12:00:00",
                   
                   end_time = "2025-04-15 23:59:59",
                   
                   flush = 180, # Flush time in seconds
                   
                   interval = 450,  # Total time at MPVPosition in seconds
                   
                   MPVPosition.levels = c("1", "2", "3"),
                   
                   location.levels = c("N", "in", "S"),
                   
                   lab = "ATB",
                   
                   analyzer = "CRDS.1")

# Read in the data
ATB_7.5_avg <- read.csv("20250408.1200-20250415.2359_ATB_450avg_CRDS.1.csv")

# Reshape to wide format, each gas and MPVPosition combination becomes a column
ATB_hourly_wide <- ATB_7.5_avg %>%
        select(-MPVPosition, -step_id, -measuring.time) %>%
        mutate(DATE.TIME = floor_date(as.POSIXct(DATE.TIME), unit = "hour")) %>%
        group_by(DATE.TIME, location, lab, analyzer) %>%
        summarise(across(where(is.numeric), ~ mean(.x, na.rm = TRUE)), .groups = "drop")%>%
        pivot_wider(
                names_from = c(location),
                values_from = c("CO2_ppm", "CO2_mgm3", "CH4_ppm", "CH4_mgm3",
                                "NH3_ppm", "NH3_mgm3", "H2O_vol"),
                names_glue = "{.value}_{location}")

# Write csv day wise
write.csv(ATB_hourly_wide,"20250408-15_ATB_hourly_wide_CRDS.1.csv" , row.names = FALSE, quote = FALSE)

####### UB Data importing and cleaning ########
#Picarro G2508
UB_CRDS.2 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/Ringversuche_2025_raw/UB_CRDS_raw",
                     
                     gas = c("CO2", "CH4", "NH3", "H2O"),
                     
                     start_time = "2025-04-08 12:00:00",
                     
                     end_time = "2025-04-15 23:59:59",
                     
                     flush = 180, # Flush time in seconds
                     
                     interval = 450,  # Total time at MPVPosition in seconds
                     
                     MPVPosition.levels = c("8", "1", "9"),
                     
                     location.levels = c("N", "in", "S"),
                     
                     lab = "UB",
                     
                     analyzer = "CRDS.2")

# Read in the data
UB_7.5_avg <- read.csv("20250408.1200-20250415.2359_UB_450avg_CRDS.2.csv")

# Reshape to wide format, each gas and MPVPosition combination becomes a column
UB_hourly_wide <- UB_7.5_avg %>%
        select(-MPVPosition, -step_id, -measuring.time) %>%
        mutate(DATE.TIME = floor_date(as.POSIXct(DATE.TIME), unit = "hour")) %>%
        group_by(DATE.TIME, location, lab, analyzer) %>%
        summarise(across(where(is.numeric), ~ mean(.x, na.rm = TRUE)), .groups = "drop")%>%
        pivot_wider(
                names_from = c(location),
                values_from = c("CO2_ppm", "CO2_mgm3", "CH4_ppm", "CH4_mgm3",
                                "NH3_ppm", "NH3_mgm3", "H2O_vol"),
                names_glue = "{.value}_{location}")

# Write csv day wise
write.csv(UB_hourly_wide,"20250408-15_UB_hourly_wide_CRDS.2.csv" , row.names = FALSE, quote = FALSE)

####### LUFA Data importing and cleaning ########
#Picarro G2508
LUFA_CRDS.3 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/Ringversuche_2025_raw/LUFA_CRDS_raw",
                       
                       gas = c("CO2", "CH4", "NH3", "H2O"),
                       
                       start_time = "2025-04-08 12:00:00",
                       
                       end_time = "2025-04-15 23:59:59",
                       
                       flush = 180, # Flush time in seconds
                       
                       interval = 450,  # Total time at MPVPosition in seconds
                       
                       MPVPosition.levels = c("3", "1", "2"),
                       
                       location.levels = c("N", "in", "S"),
                       
                       lab = "LUFA",
                       
                       analyzer = "CRDS.3")

# Read in the data
LUFA_7.5_avg <- read.csv("20250408.1200-20250415.2359_LUFA_450avg_CRDS.3.csv")

# Reshape to wide format, each gas and MPVPosition combination becomes a column
LUFA_hourly_wide <- LUFA_7.5_avg %>%
        select(-MPVPosition, -step_id, -measuring.time) %>%
        mutate(DATE.TIME = floor_date(as.POSIXct(DATE.TIME), unit = "hour")) %>%
        group_by(DATE.TIME, location, lab, analyzer) %>%
        summarise(across(where(is.numeric), ~ mean(.x, na.rm = TRUE)), .groups = "drop")%>%
        pivot_wider(
                names_from = c(location),
                values_from = c("CO2_ppm", "CO2_mgm3", "CH4_ppm", "CH4_mgm3",
                                "NH3_ppm", "NH3_mgm3", "H2O_vol"),
                names_glue = "{.value}_{location}")

write.csv(LUFA_hourly_wide,"20250408-15_LUFA_hourly_wide_CRDS.3.csv" , row.names = FALSE, quote = FALSE)
