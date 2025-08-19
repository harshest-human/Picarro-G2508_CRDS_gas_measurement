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

####### 2025-05-08 to 2025-05-15 ATB Data importing and cleaning ########
CRDS9_20250508 <- piclean(
        input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509",
        gas = c("CO2", "CH4", "NH3", "H2O", "N2O"),
        start_time = "2025-05-08 12:00:00",
        end_time   = "2025-05-20 13:50:00",
        flush = 180, # in seconds
        interval = 450, # in seconds
)

# Create an hourly timestamp to group by
CRDS9_20250508$DATE.TIME <- as.POSIXct(CRDS9_20250508$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")
CRDS9_20250508$DATE.TIME <- floor_date(CRDS9_20250508$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
CRDS9_20250508_avg <- CRDS9_20250508 %>%
        select(-step_id) %>%
        group_by(DATE.TIME, MPVPosition) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                N2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )

CRDS9_20250508_avg <- CRDS9_20250508_avg %>%
        filter(MPVPosition %in% c(1, 2, 3)) %>%
        mutate(
                location = recode(as.factor(MPVPosition),
                                  `1` = "N",
                                  `2` = "in",
                                  `3` = "S"),
                lab = factor("ATB"),
                analyzer = factor("CRDS9")
        )

# Write long csv
CRDS9_20250508_avg <- CRDS9_20250508_avg %>% select(DATE.TIME, MPVPosition, location, lab, analyzer, everything())
CRDS9_20250508_avg$DATE.TIME <- format(CRDS9_20250508_avg$DATE.TIME, "%Y-%m-%d %H:%M:%S")
write.csv(CRDS9_20250508_avg,"ATB_CRDS9_20250508_hourly_gas_conc.csv" , row.names = FALSE, quote = FALSE)

# Change pivot to wide
CRDS9_20250508_inout <- CRDS9_20250508_avg %>%
        select(-MPVPosition) %>% 
        pivot_wider(
                names_from = location,
                values_from = c(CO2, CH4, NH3, H2O, N2O),
                names_sep = "_"
        )

# Write wide csv
write.csv(CRDS9_20250508_inout,"CRDS9_20250508_inout.csv" , row.names = FALSE, quote = FALSE)


####### 2025-05-22 to 2025-05-31 ATB Data importing and cleaning ########
CRDS9_20250522 <- piclean(
        input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509/2025",
        gas = c("CO2", "CH4", "NH3", "H2O", "N2O"),
        start_time = "2025-05-22 13:00:00",
        end_time   = "2025-06-23 11:55:00",
        flush = 180, # in seconds
        interval = 450) # in seconds

# Create an hourly timestamp to group by
CRDS9_20250522$DATE.TIME <- as.POSIXct(CRDS9_20250522$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")
CRDS9_20250522$DATE.TIME <- floor_date(CRDS9_20250522$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
CRDS9_20250522_avg <- CRDS9_20250522 %>%
        select(-step_id) %>%
        group_by(DATE.TIME, MPVPosition) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                N2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )

CRDS9_20250522_avg <- CRDS9_20250522_avg %>%
        filter(MPVPosition %in% c(1, 2, 3)) %>%
        mutate(
                location = recode(as.factor(MPVPosition),
                                  `1` = "N",
                                  `2` = "in",
                                  `3` = "S"),
                lab = factor("ATB"),
                analyzer = factor("CRDS9")
        )

# Write long csv
CRDS9_20250522_avg <- CRDS9_20250522_avg %>% select(DATE.TIME, MPVPosition, location, lab, analyzer, everything())
CRDS9_20250522_avg$DATE.TIME <- format(CRDS9_20250522_avg$DATE.TIME, "%Y-%m-%d %H:%M:%S")
write.csv(CRDS9_20250522_avg,"ATB_CRDS9_20250522_hourly_gas_conc.csv" , row.names = FALSE, quote = FALSE)

# Change pivot to wide
CRDS9_20250522_inout <- CRDS9_20250522_avg %>%
        select(-MPVPosition) %>% 
        pivot_wider(
                names_from = location,
                values_from = c(CO2, CH4, NH3, H2O, N2O),
                names_sep = "_"
        )

# Write wide csv
write.csv(CRDS9_20250522_inout,"CRDS9_20250522_inout.csv" , row.names = FALSE, quote = FALSE)


####### 2025-06-26 to 2025-07-10 ATB Data importing and cleaning ########
CRDS9_20250626 <- piclean(
        input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509/2025",
        gas = c("CO2", "CH4", "NH3", "H2O", "N2O"),
        start_time = "2025-06-26 11:00:00",
        end_time   = "2025-07-10 18:07:25",
        flush = 180, # in seconds
        interval = 450) # in seconds

# Create an hourly timestamp to group by
CRDS9_20250626$DATE.TIME <- as.POSIXct(CRDS9_20250626$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")
CRDS9_20250626$DATE.TIME <- floor_date(CRDS9_20250626$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
CRDS9_20250626_avg <- CRDS9_20250626 %>%
        select(-step_id) %>%
        group_by(DATE.TIME, MPVPosition) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                N2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )

CRDS9_20250626_avg <- CRDS9_20250626_avg %>%
        filter(MPVPosition %in% c(1, 2, 3)) %>%
        mutate(
                location = recode(as.factor(MPVPosition),
                                  `1` = "N",
                                  `2` = "in",
                                  `3` = "S"),
                lab = factor("ATB"),
                analyzer = factor("CRDS9")
        )

# Write long csv
CRDS9_20250626_avg <- CRDS9_20250626_avg %>% select(DATE.TIME, MPVPosition, location, lab, analyzer, everything())
CRDS9_20250626_avg$DATE.TIME <- format(CRDS9_20250626_avg$DATE.TIME, "%Y-%m-%d %H:%M:%S")
write.csv(CRDS9_20250626_avg,"ATB_CRDS9_20250626_hourly_gas_conc.csv" , row.names = FALSE, quote = FALSE)

# Change pivot to wide
CRDS9_20250626_inout <- CRDS9_20250626_avg %>%
        select(-MPVPosition) %>% 
        pivot_wider(
                names_from = location,
                values_from = c(CO2, CH4, NH3, H2O, N2O),
                names_sep = "_"
        )

# Write wide csv
write.csv(CRDS9_20250626_inout,"CRDS9_20250626_inout.csv" , row.names = FALSE, quote = FALSE)


####### 2025-07-10 to 2025-07-24 ATB Data importing and cleaning ########
CRDS9_20250710 <- piclean(
        input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509/2025",
        gas = c("CO2", "CH4", "NH3", "H2O", "N2O"),
        start_time = "2025-07-10 18:07:25",
        end_time   = "2025-07-24 18:07:25",
        flush = 180, # in seconds
        interval = 450) # in seconds

# Create an hourly timestamp to group by
CRDS9_20250710$DATE.TIME <- as.POSIXct(CRDS9_20250710$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")
CRDS9_20250710$DATE.TIME <- floor_date(CRDS9_20250710$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
CRDS9_20250710_avg <- CRDS9_20250710 %>%
        select(-step_id) %>%
        group_by(DATE.TIME, MPVPosition) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                N2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )

CRDS9_20250710_avg <- CRDS9_20250710_avg %>%
        filter(MPVPosition %in% c(1, 2, 3)) %>%
        mutate(
                location = recode(as.factor(MPVPosition),
                                  `1` = "N",
                                  `2` = "in",
                                  `3` = "S"),
                lab = factor("ATB"),
                analyzer = factor("CRDS9")
        )

# Write long csv
CRDS9_20250710_avg <- CRDS9_20250710_avg %>% select(DATE.TIME, MPVPosition, location, lab, analyzer, everything())
CRDS9_20250710_avg$DATE.TIME <- format(CRDS9_20250710_avg$DATE.TIME, "%Y-%m-%d %H:%M:%S")
write.csv(CRDS9_20250710_avg,"ATB_CRDS9_20250710_hourly_gas_conc.csv" , row.names = FALSE, quote = FALSE)

# Change pivot to wide
CRDS9_20250710_inout <- CRDS9_20250710_avg %>%
        select(-MPVPosition) %>% 
        pivot_wider(
                names_from = location,
                values_from = c(CO2, CH4, NH3, H2O, N2O),
                names_sep = "_"
        )

# Write wide csv
write.csv(CRDS9_20250710_inout,"CRDS9_20250710_inout.csv" , row.names = FALSE, quote = FALSE)

####### 2025-07-24 t 2025-08-19 ATB Data importing and cleaning ########
CRDS9_20250724 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509/2025",
                       
                       gas = c("CO2", "CH4", "NH3", "H2O"),
                       
                       start_time = "2025-07-24 18:07:25",
                       
                       end_time = "2025-08-19 01:37:56",
                       
                       flush = 180, # Flush time in seconds
                       
                       interval = 450,  # Total time at MPVPosition in seconds
                       
                       MPVPosition.levels = c("1", "2", "3"),
                       
                       location.levels = c("N", "in", "S"),
                       
                       lab = "ATB",
                       
                       analyzer = "CRDS9")


# Create an hourly timestamp to group by
CRDS9_20250724$DATE.TIME <- as.POSIXct(CRDS9_20250724$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")
CRDS9_20250724$DATE.TIME <- floor_date(CRDS9_20250724$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
CRDS9_20250724_avg <- CRDS9_20250724 %>%
        select(-step_id) %>%
        group_by(DATE.TIME, MPVPosition, location, lab, analyzer) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                N2O = mean(H2O, na.rm = TRUE),
                .groups = "drop")  # To avoid warning about grouping


# Write long csv
CRDS9_20250724_avg$DATE.TIME <- format(CRDS9_20250724_avg$DATE.TIME, "%Y-%m-%d %H:%M:%S")
write.csv(CRDS9_20250724_avg,"ATB_CRDS9_20250724_hourly_gas_conc.csv" , row.names = FALSE, quote = FALSE)

# Change pivot to wide
CRDS9_20250724_inout <- CRDS9_20250724_avg %>%
        select(-MPVPosition) %>% 
        pivot_wider(names_from = location,
                    values_from = c(CO2, CH4, NH3, H2O, N2O),
                    names_sep = "_")
                

# Write wide csv
write.csv(CRDS9_20250724_inout,"CRDS9_20250724_inout.csv" , row.names = FALSE, quote = FALSE)

