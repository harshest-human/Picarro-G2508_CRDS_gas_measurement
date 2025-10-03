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
library(data.table)
source("Picarro_CRDS_data_cleaning_script.R")

####### 2025-09-25 to 2025-09-30 ATB Data importing and cleaning ########
CRDS9_20250925_20250930 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509/2025",
                          
                          gas = c("CO2", "CH4", "NH3", "H2O", "N2O"),
                          
                          start_time = "2025-09-25 12:13:43",
                          
                          end_time = "2025-09-30 23:30:44",
                          
                          flush = 60, # Flush time in seconds
                          
                          interval = 240,  # Total time at MPVPosition in seconds
                          
                          MPVPosition.levels = c("1", "2", "3", "4", "5", "6", "7", "8",
                                                 "9", "10", "11", "12", "13", "14", "15", "16"),
                          
                          location.levels = c("1", "3", "4", "6", "7", "9", "10", "12",
                                              "13", "15", "16", "18", "22", "24", "in", "S"),
                          
                          lab = "ATB",
                          
                          analyzer = "CRDS9")


CRDS8_20250925_20250930 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2025",
                          
                          gas = c("CO2", "CH4", "NH3", "H2O", "N2O"),
                          
                          start_time = "2025-09-25 12:06:05",
                          
                          end_time = "2025-09-30 23:19:33",
                             
                          flush = 60, # Flush time in seconds
                          
                          interval = 240,  # Total time at MPVPosition in seconds
                          
                          MPVPosition.levels = c("1", "2", "3", "4", "5", "6", "7", "8", 
                                                 "9", "10", "11", "12", "13", "14", "15", "16"),
                          
                          location.levels = c("28", "30", "31", "33", "34", "36", "37", "39",
                                              "40", "42", "43", "45", "46", "48", "49", "51"),
                          
                          lab = "ATB",
                          
                          analyzer = "CRDS8")

# Combine both CRDS
CRDS_20250925_20250930_combined_df <- rbind(CRDS9_20250925_20250930, CRDS8_20250925_20250930) %>%
        arrange(DATE.TIME)

# Keep long format for dummy grid + join
CRDS_20250925_20250930_hourly_long <- CRDS_20250925_20250930_combined_df %>%
        filter(location %in% c("in", "S")) %>%
        mutate(step_id_S = step_id) %>%
        left_join(
                CRDS_20250925_20250930_combined_df %>%
                        filter(location == "in") %>%
                        transmute(step_id_S = step_id + 1, DATE.TIME_in = DATE.TIME, lab, analyzer),
                by = c("step_id_S", "lab", "analyzer")
        ) %>%
        mutate(DATE.TIME = if_else(location == "S" & !is.na(DATE.TIME_in), DATE.TIME_in, DATE.TIME)) %>%
        select(-DATE.TIME_in, -step_id_S) %>%
        mutate(DATE.HOUR = floor_date(DATE.TIME, "hour")) %>%
        group_by(DATE.HOUR, location, lab, analyzer) %>%
        summarise(across(c(CO2, CH4, NH3, H2O, N2O), ~mean(.x, na.rm = TRUE)), .groups = "drop")

# Step 2: full grid still has location
CRDS_20250925_20250930_dummy <- expand_grid(
        DATE.HOUR = seq(min(CRDS_20250925_20250930_hourly_long$DATE.HOUR),
                        max(CRDS_20250925_20250930_hourly_long$DATE.HOUR), by = "hour"),
        analyzer = unique(CRDS_20250925_20250930_hourly_long$analyzer),
        location = unique(CRDS_20250925_20250930_hourly_long$location),
        lab      = unique(CRDS_20250925_20250930_hourly_long$lab))

# Step 3: Join works, because location exists
CRDS_20250925_20250930_hourly_wide <- CRDS_20250925_20250930_dummy %>%
        left_join(CRDS_20250925_20250930_hourly_long,
                  by = c("DATE.HOUR", "location", "analyzer", "lab")) %>%
        pivot_wider(
                names_from = location,
                values_from = c(CO2, CH4, NH3, H2O, N2O),
                names_sep = "_")


write_excel_csv(CRDS_20250925_20250930_hourly_wide, "H_CRDS9_20250925_20250930.csv")


#Data Visualization
ggline(CRDS_combined_df, 
       x = "location", 
       y = "CO2",
       add = "mean_se",         # Use mean ± SD
       error.plot = "errorbar", # Show error bars
       ylab = "Mean CO2 (ppm)",
       xlab = "MPV Position",
       title = "Mean CO2 ± SD by MPVPosition",
       color = "steelblue",
       add.params = list(width = 0.2)) +
        theme_minimal()

ggline(CRDS_combined_df, 
       x = "location", 
       y = "CH4",
       add = "mean_se",         # Use mean ± SD
       error.plot = "errorbar", # Show error bars
       ylab = "Mean CH4 (ppm)",
       xlab = "MPV Position",
       title = "Mean CH4 ± SD by MPVPosition",
       color = "green4",
       add.params = list(width = 0.2)) +
        theme_minimal()

ggline(CRDS_combined_df, 
       x = "location", 
       y = "NH3",
       add = "mean_se",         # Use mean ± SD
       error.plot = "errorbar", # Show error bars
       ylab = "Mean NH3 (ppm)",
       xlab = "MPV Position",
       title = "Mean NH3 ± SD by MPVPosition",
       color = "orange4",
       add.params = list(width = 0.2)) +
        theme_minimal()
