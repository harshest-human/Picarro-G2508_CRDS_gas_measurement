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
ATB_CRDS.1 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2025/04",
                   
                   output_path = "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean",
                   
                   result_file_name = "20250408-15_Ring_7.5_cycle_ATB_CRDS.1",
                   
                   gas = c("CO2", "CH4", "NH3", "H2O"),
                   
                   start_time = "2025-04-08 12:00:00",
                   
                   end_time = "2025-04-15 12:59:59",
                   
                   flush = 180, # Flush time in seconds
                   
                   interval = 450) # Total time at MPVPosition in seconds


# Read in the data
ATB_CRDS.1 <- read.csv("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/20250408-15_Ring_7.5_cycle_ATB_CRDS.1.csv")

# Create an hourly timestamp to group by
ATB_CRDS.1$DATE.TIME <- as.POSIXct(ATB_CRDS.1$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")
ATB_CRDS.1$DATE.TIME <- floor_date(ATB_CRDS.1$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
ATB_avg <- ATB_CRDS.1 %>%
        group_by(DATE.TIME, MPVPosition) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )%>%
        mutate(
                CO2 = CO2 * 37.2,    # ppm to mg/m3 for CO2
                CH4 = CH4 * 13.6,   # ppm to mg/m3 for CH4
                NH3 = NH3 * 14.4,  # ppm to mg/m3 for NH3
        ) 

ATB_avg <- ATB_avg %>%
        filter(MPVPosition %in% c(1, 2, 3)) %>%
        mutate(
                location = recode(as.factor(MPVPosition),
                                  `1` = "N",
                                  `2` = "in",
                                  `3` = "S"),
                lab = factor("ATB"),
                analyzer = factor("CRDS.1")
        )

# Write csv
ATB_avg <- ATB_avg %>% select(DATE.TIME, MPVPosition, location, lab, analyzer, everything())
ATB_avg$DATE.TIME <- format(ATB_avg$DATE.TIME, "%Y-%m-%d %H:%M:%S")
write.csv(ATB_avg,"20250408-15_hourly_ATB_CRDS.1.csv" , row.names = FALSE, quote = FALSE)

# Reshape to wide format, each gas and MPVPosition combination becomes a column
ATB_long <- ATB_avg %>%
        select(-MPVPosition) %>%
        pivot_wider(
                names_from = c(location),
                values_from = c(CO2, CH4, NH3, H2O),
                names_glue = "{.value}_{location}"
        )


# Write csv day wise
write.csv(ATB_long,"20250408-15_long_ATB_CRDS.1.csv" , row.names = FALSE, quote = FALSE)


####### UB Data importing and cleaning ########
#Picarro G2508
UB_CRDS.2 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/Ringversuche_2025_raw/UB_CRDS_raw",
                      
                      output_path = "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean",
                      
                      result_file_name = "20250408-15_Ring_7.5_cycle_UB_CRDS.2",
                      
                      gas = c("CO2", "CH4", "NH3", "H2O"),
                      
                      start_time = "2025-04-08 12:00:00",
                      
                      end_time = "2025-04-15 12:59:59",
                      
                      flush = 180, # Flush time in seconds
                      
                      interval = 450) # Total time at MPVPosition in seconds



# Read in the data
UB_CRDS.2 <- read.csv("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/20250408-15_Ring_7.5_cycle_UB_CRDS.2.csv")

# Create an hourly timestamp to group by
UB_CRDS.2$DATE.TIME <- as.POSIXct(UB_CRDS.2$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")
UB_CRDS.2$DATE.TIME <- floor_date(UB_CRDS.2$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
UB_avg <- UB_CRDS.2 %>%
        group_by(DATE.TIME, MPVPosition) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )%>%
        mutate(
                CO2 = CO2 * 37.2,    # ppm to mg/m3 for CO2
                CH4 = CH4 * 13.6,   # ppm to mg/m3 for CH4
                NH3 = NH3 * 14.4,  # ppm to mg/m3 for NH3
        ) 

UB_avg <- UB_avg %>%
        filter(MPVPosition %in% c(1, 8, 9)) %>%
        mutate(
                location = recode(as.factor(MPVPosition),
                                  `8` = "N",
                                  `1` = "in",
                                  `9` = "S"),
                lab = factor("UB"),
                analyzer = factor("CRDS.2")
        )

# Write csv
UB_avg <- UB_avg %>% select(DATE.TIME, MPVPosition, location, lab, analyzer, everything())
UB_avg$DATE.TIME <- format(UB_avg$DATE.TIME, "%Y-%m-%d %H:%M:%S")
write.csv(UB_avg,"20250408-15_hourly_UB_CRDS.2.csv" , row.names = FALSE, quote = FALSE)


# Reshape to wide format, each gas and MPVPosition combination becomes a column
UB_long <- UB_avg %>%
        select(-MPVPosition) %>%
        pivot_wider(
                names_from = c(location),
                values_from = c(CO2, CH4, NH3, H2O),
                names_glue = "{.value}_{location}"
        )


# Write csv day wise
write.csv(UB_long,"20250408-15_long_UB_CRDS.2.csv" , row.names = FALSE, quote = FALSE)


####### UB Data importing and cleaning ########
#Picarro G2508
LUFA_CRDS.3 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/Ringversuche_2025_raw/LUFA_CRDS_raw",
                        
                        output_path = "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean",
                        
                        result_file_name = "20250408-15_Ring_7.5_cycle_LUFA_CRDS.3",
                        
                        gas = c("CO2", "CH4", "NH3", "H2O"),
                        
                        start_time = "2025-04-08 12:00:00",
                        
                        end_time = "2025-04-15 12:59:59",
                        
                        flush = 180, # Flush time in seconds
                        
                        interval = 450) # Total time at MPVPosition in seconds


# Read in the data
LUFA_CRDS.3 <- read.csv("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/20250408-15_Ring_7.5_cycle_LUFA_CRDS.3.csv")

# Create an hourly timestamp to group by
LUFA_CRDS.3$DATE.TIME <- as.POSIXct(LUFA_CRDS.3$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")
LUFA_CRDS.3$DATE.TIME <- floor_date(LUFA_CRDS.3$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
LUFA_avg <- LUFA_CRDS.3 %>%
        group_by(DATE.TIME, MPVPosition) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )%>%
        mutate(
                CO2 = CO2 * 37.2,    # ppm to mg/m3 for CO2
                CH4 = CH4 * 13.6,   # ppm to mg/m3 for CH4
                NH3 = NH3 * 14.4,  # ppm to mg/m3 for NH3
        ) 

LUFA_avg <- LUFA_avg %>%
        filter(MPVPosition %in% c(1, 2, 3)) %>%
        mutate(
                location = recode(as.factor(MPVPosition),
                                  `1` = "in",
                                  `2` = "S",
                                  `3` = "N"),
                lab = factor("LUFA"),
                analyzer = factor("CRDS.3")
        )

# Write csv
LUFA_avg <- LUFA_avg %>% select(DATE.TIME, MPVPosition, location, lab, analyzer, everything())
LUFA_avg$DATE.TIME <- format(LUFA_avg$DATE.TIME, "%Y-%m-%d %H:%M:%S")
write.csv(LUFA_avg,"20250408-15_hourly_LUFA_CRDS.3.csv" , row.names = FALSE, quote = FALSE)

# Reshape to wide format, each gas and MPVPosition combination becomes a column
LUFA_long <- LUFA_avg %>%
        select(-MPVPosition) %>%
        pivot_wider(
                names_from = c(location),
                values_from = c(CO2, CH4, NH3, H2O),
                names_glue = "{.value}_{location}"
        )


# Write csv day wise
write.csv(LUFA_long,"20250408-15_long_LUFA_CRDS.3.csv" , row.names = FALSE, quote = FALSE)
