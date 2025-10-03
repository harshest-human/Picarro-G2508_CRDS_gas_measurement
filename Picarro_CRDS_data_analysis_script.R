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
source("remove_outliers_function.R")
source("round to interval function.R")


####### ATB Data importing and cleaning ########
#Picarro G2508
ATB_7.5_avg <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/Ringversuche_2025_raw/ATB_CRDS_raw/2025",
                      
                   gas = c("CO2", "CH4", "NH3", "H2O", "N2O"),
                   
                   start_time = "2025-04-08 12:00:00",
                   
                   end_time = "2025-04-14 13:00:00",
                   
                   flush = 180, # Flush time in seconds
                   
                   interval = 450,  # Total time at MPVPosition in seconds
                   
                   MPVPosition.levels = c("1", "2", "3"),
                   
                   location.levels = c("N", "in", "S"),
                   
                   lab = "ATB",
                   
                   analyzer = "CRDS.1")

#Round DATE.TIME to the nearest 450 seconds (7.5 minutes)
ATB_7.5_avg <- ATB_7.5_avg %>%
        mutate(DATE.TIME = ymd_hms(DATE.TIME),
               DATE.TIME = round_to_interval(DATE.TIME, interval_sec = 450)) %>%
        select(DATE.TIME, location, lab, analyzer, everything(), -step_id, -MPVPosition, -measuring.time)

# Remove outliers 
ATB_7.5_avg <- ATB_7.5_avg %>% 
        remove_outliers(exclude_cols = c("DATE.TIME", "lab", "analyzer"),
        group_cols = c("location"))

# Write csv
write.csv(ATB_7.5_avg,"20250408-14_ATB_7.5_avg_CRDS.1.csv" , row.names = FALSE, quote = FALSE)

# hourly averaged intervals long format
# Calculate hourly mean and change pivot to long
ATB_long <- ATB_7.5_avg %>%
        mutate(DATE.TIME = ymd_hms(DATE.TIME)) %>%
        mutate(DATE.TIME = floor_date(DATE.TIME, unit = "hour")) %>%
        group_by(DATE.TIME, location, analyzer, lab) %>%
        summarise(CO2_ppm = mean(CO2, na.rm = TRUE),
                  CH4_ppm = mean(CH4, na.rm = TRUE),
                  NH3_ppm = mean(NH3, na.rm = TRUE),
                  N2O_ppm = mean(N2O, na.rm = TRUE),
                  H2O_vol = mean(H2O, na.rm = TRUE),
                  .groups = "drop")%>%
        pivot_longer(cols = c(CO2_ppm, CH4_ppm, NH3_ppm, N2O_ppm, H2O_vol),
                     names_to = "var_unit",
                     values_to = "value")

# Write csv long
write_excel_csv(ATB_long,"20250408-14_ATB_long_CRDS.1.csv")       


# hourly averaged intervals wide format
# Reshape to wide format, each gas and Line combination becomes a column
ATB_wide <- ATB_long %>%
        pivot_wider(
                names_from = c(var_unit, location),
                values_from = value,
                names_sep = "_") %>%
        arrange(DATE.TIME)

# Write csv wide
write_excel_csv(ATB_wide,"20250408-14_ATB_wide_CRDS.1.csv")    


####### UB Data importing and cleaning ########
#Picarro G2508
UB_7.5_avg <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/Ringversuche_2025_raw/UB_CRDS_raw",
                     
                     gas = c("CO2", "CH4", "NH3", "H2O","N2O"),
                     
                     start_time = "2025-04-08 12:00:00",
                     
                     end_time = "2025-04-14 13:00:00",
                     
                     flush = 180, # Flush time in seconds
                     
                     interval = 450,  # Total time at MPVPosition in seconds
                     
                     MPVPosition.levels = c("8", "1", "9"),
                     
                     location.levels = c("N", "in", "S"),
                     
                     lab = "UB",
                     
                     analyzer = "CRDS.2")

# Read in the data
UB_7.5_avg$DATE.TIME <- as.POSIXct(UB_7.5_avg$DATE.TIME, 
                                     format = "%Y-%m-%d %H:%M:%S")

UB_7.5_avg <- UB_7.5_avg %>%
        mutate(DATE.TIME = ymd_hms(DATE.TIME),
               DATE.TIME = round_to_interval(DATE.TIME, interval_sec = 450)) %>%
        select(DATE.TIME, location, lab, analyzer, everything(), -step_id, -MPVPosition, -measuring.time)

# Remove outliers 
UB_7.5_avg <- UB_7.5_avg %>% 
        remove_outliers(exclude_cols = c("DATE.TIME", "lab", "analyzer"),
                        group_cols = c("location"))

# Write csv
write.csv(UB_7.5_avg,"20250408-14_UB_7.5_avg_CRDS.2.csv" , row.names = FALSE, quote = FALSE)

# hourly averaged intervals long format
# Calculate hourly mean and change pivot to long
UB_long <- UB_7.5_avg %>%
        mutate(DATE.TIME = ymd_hms(DATE.TIME)) %>%
        mutate(DATE.TIME = floor_date(DATE.TIME, unit = "hour")) %>%
        group_by(DATE.TIME, location, analyzer, lab) %>%
        summarise(CO2_ppm = mean(CO2, na.rm = TRUE),
                  CH4_ppm = mean(CH4, na.rm = TRUE),
                  NH3_ppm = mean(NH3, na.rm = TRUE),
                  N2O_ppm = mean(N2O, na.rm = TRUE),
                  H2O_vol = mean(H2O, na.rm = TRUE),
                  .groups = "drop")%>%
        pivot_longer(cols = c(CO2_ppm, CH4_ppm, NH3_ppm, N2O_ppm, H2O_vol),
                     names_to = "var_unit",
                     values_to = "value")

# Write csv long
write_excel_csv(UB_long,"20250408-14_UB_long_CRDS.2.csv")       

# hourly averaged intervals wide format
# Reshape to wide format, each gas and Line combination becomes a column
UB_wide <- UB_long %>%
        pivot_wider(
                names_from = c(var_unit, location),
                values_from = value,
                names_sep = "_") %>%
        arrange(DATE.TIME)

# Write csv wide
write_excel_csv(UB_wide,"20250408-14_UB_wide_CRDS.2.csv")

####### LUFA Data importing and cleaning ########
#Picarro G2508
LUFA_7.5_avg <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/Ringversuche_2025_raw/LUFA_CRDS_raw",
                       
                       gas = c("CO2", "CH4", "NH3", "H2O","N2O"),
                       
                       start_time = "2025-04-08 12:00:00",
                       
                       end_time = "2025-04-14 13:00:00",
                       
                       flush = 180, # Flush time in seconds
                       
                       interval = 450,  # Total time at MPVPosition in seconds
                       
                       MPVPosition.levels = c("3", "1", "2"),
                       
                       location.levels = c("N", "in", "S"),
                       
                       lab = "LUFA",
                       
                       analyzer = "CRDS.3")

# Read in the data
LUFA_7.5_avg$DATE.TIME <- as.POSIXct(LUFA_7.5_avg$DATE.TIME, 
                                    format = "%Y-%m-%d %H:%M:%S") - 20 # time offsetting


LUFA_7.5_avg <- LUFA_7.5_avg %>%
        mutate(DATE.TIME = ymd_hms(DATE.TIME),
               DATE.TIME = round_to_interval(DATE.TIME, interval_sec = 450)) %>%
        select(DATE.TIME, location, lab, analyzer, everything(), -step_id, -MPVPosition, -measuring.time)


# Remove outliers 
LUFA_7.5_avg <- LUFA_7.5_avg %>% 
        remove_outliers(exclude_cols = c("DATE.TIME", "lab", "analyzer"),
                        group_cols = c("location"))

# Write csv
write.csv(LUFA_7.5_avg,"20250408-14_LUFA_7.5_avg_CRDS.3.csv" , row.names = FALSE, quote = FALSE)

# hourly averaged intervals long format
# Calculate hourly mean and change pivot to long
LUFA_long <- LUFA_7.5_avg %>%
        mutate(DATE.TIME = ymd_hms(DATE.TIME)) %>%
        mutate(DATE.TIME = floor_date(DATE.TIME, unit = "hour")) %>%
        group_by(DATE.TIME, location, analyzer, lab) %>%
        summarise(CO2_ppm = mean(CO2, na.rm = TRUE),
                  CH4_ppm = mean(CH4, na.rm = TRUE),
                  NH3_ppm = mean(NH3, na.rm = TRUE),
                  N2O_ppm = mean(N2O, na.rm = TRUE),
                  H2O_vol = mean(H2O, na.rm = TRUE),
                  .groups = "drop")%>%
        pivot_longer(cols = c(CO2_ppm, CH4_ppm, NH3_ppm, N2O_ppm, H2O_vol),
                     names_to = "var_unit",
                     values_to = "value")

# Write csv long
write_excel_csv(LUFA_long,"20250408-14_LUFA_long_CRDS.3.csv")       

# hourly averaged intervals wide format
# Reshape to wide format, each gas and Line combination becomes a column
LUFA_wide <- LUFA_long %>%
        pivot_wider(
                names_from = c(var_unit, location),
                values_from = value,
                names_sep = "_") %>%
        arrange(DATE.TIME)

# Write csv wide
write_excel_csv(LUFA_wide,"20250408-14_LUFA_wide_CRDS.3.csv")    
