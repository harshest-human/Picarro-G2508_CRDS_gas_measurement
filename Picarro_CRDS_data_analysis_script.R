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
ATB_CRDS.P8$DATE.TIME <- floor_date(ATB_CRDS.P8$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
ATB_avg <- ATB_CRDS.P8 %>%
        group_by(DATE.TIME, MPVPosition) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )

ATB_avg <- ATB_avg %>%
        filter(MPVPosition %in% c(1, 2, 3)) %>%
        mutate(
                location = recode(as.factor(MPVPosition),
                                  `1` = "N",
                                  `2` = "in",
                                  `3` = "S"),
                lab = factor("ATB"),
                analyzer = factor("CRDS.P8")
        )

# Write csv
ATB_avg <- ATB_avg %>% select(DATE.TIME, MPVPosition, location, lab, analyzer, everything())
write.csv(ATB_avg,"20250408-15_hourly_ATB_CRDS.P8.csv" , row.names = FALSE, quote = FALSE)

# Reshape to wide format, each gas and MPVPosition combination becomes a column
ATB_long <- ATB_avg %>%
        select(-MPVPosition) %>%
        pivot_wider(
                names_from = c(location,lab),
                values_from = c(CO2, CH4, NH3, H2O),
                names_glue = "{.value}_{location}_{lab}"
        )

# Convert DATE.TIME to datetime format
ATB_long$DATE.TIME <- ymd_hms(ATB_long$DATE.TIME)

# Write csv day wise
write.csv(ATB_long,"20250408-15_long_ATB_CRDS.P8.csv" , row.names = FALSE, quote = FALSE)


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
UB_CRDS.P8$DATE.TIME <- floor_date(UB_CRDS.P8$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
UB_avg <- UB_CRDS.P8 %>%
        group_by(DATE.TIME, MPVPosition) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )

UB_avg <- UB_avg %>%
        filter(MPVPosition %in% c(1, 8, 9)) %>%
        mutate(
                location = recode(as.factor(MPVPosition),
                                  `8` = "N",
                                  `1` = "in",
                                  `9` = "S"),
                lab = factor("UB"),
                analyzer = factor("CRDS.P8")
        )

# Write csv
UB_avg <- UB_avg %>% select(DATE.TIME, MPVPosition, location, lab, analyzer, everything())
write.csv(UB_avg,"20250408-15_hourly_UB_CRDS.P8.csv" , row.names = FALSE, quote = FALSE)


# Reshape to wide format, each gas and MPVPosition combination becomes a column
UB_long <- UB_avg %>%
        select(-MPVPosition) %>%
        pivot_wider(
                names_from = c(location,lab),
                values_from = c(CO2, CH4, NH3, H2O),
                names_glue = "{.value}_{location}_{lab}"
        )


# Convert DATE.TIME to datetime format
UB_long$DATE.TIME <- ymd_hms(UB_long$DATE.TIME)

# Write csv day wise
write.csv(UB_long,"20250408-15_long_UB_CRDS.P8.csv" , row.names = FALSE, quote = FALSE)



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
LUFA_CRDS.P8$DATE.TIME <- floor_date(LUFA_CRDS.P8$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
LUFA_avg <- LUFA_CRDS.P8 %>%
        group_by(DATE.TIME, MPVPosition) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )

LUFA_avg <- LUFA_avg %>%
        filter(MPVPosition %in% c(1, 2, 3)) %>%
        mutate(
                location = recode(as.factor(MPVPosition),
                                  `1` = "in",
                                  `2` = "S",
                                  `3` = "N"),
                lab = factor("LUFA"),
                analyzer = factor("CRDS.P8")
        )

# Write csv
LUFA_avg <- LUFA_avg %>% select(DATE.TIME, MPVPosition, location, lab, analyzer, everything())
write.csv(LUFA_avg,"20250408-15_hourly_LUFA_CRDS.P8.csv" , row.names = FALSE, quote = FALSE)

# Reshape to wide format, each gas and MPVPosition combination becomes a column
LUFA_long <- LUFA_avg %>%
        select(-MPVPosition) %>%
        pivot_wider(
                names_from = c(location,lab),
                values_from = c(CO2, CH4, NH3, H2O),
                names_glue = "{.value}_{location}_{lab}"
        )

# Convert DATE.TIME to datetime format
LUFA_long$DATE.TIME <- ymd_hms(LUFA_long$DATE.TIME)

# Write csv day wise
write.csv(LUFA_long,"20250408-15_long_LUFA_CRDS.P8.csv" , row.names = FALSE, quote = FALSE)
