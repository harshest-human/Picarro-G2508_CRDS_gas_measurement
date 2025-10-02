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

# Step 1: Filter in and S, copy in's DATE.TIME to S
CRDS_20250925_20250930_in_S <- CRDS_20250925_20250930_combined_df %>%
        filter(location %in% c("in", "S")) %>%
        mutate(DATE.HOUR = floor_date(DATE.TIME, "hour")) %>%
        group_by(DATE.HOUR, lab, analyzer) %>%
        mutate(
                in_time = suppressWarnings(first(DATE.TIME[location == "in"])),
                DATE.TIME = ifelse(location == "S" & !is.na(in_time),
                                   in_time,
                                   DATE.TIME)
        ) %>%
        ungroup() %>%
        select(-in_time)




# Step 1: Hourly aggregation (averaged across all locations)
CRDS_20250925_20250930_hourly <- CRDS_20250925_20250930_combined_df %>%
        filter(location %in% c("in", "S")) %>%
        mutate(DATE.HOUR = floor_date(DATE.TIME, "hour")) %>%
        select(-step_id, -MPVPosition, -measuring.time) %>%
        group_by(DATE.HOUR, location, lab, analyzer) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                N2O = mean(N2O, na.rm = TRUE),
                .groups = "drop") %>%
        mutate(across(c(location, analyzer, lab), as.character))


# Step 2: Create full grid of DATE.HOUR × analyzer × lab
CRDS_20250925_20250930_dummy <- expand_grid(
        DATE.HOUR = seq(
                from = floor_date(min(CRDS_20250925_20250930_hourly$DATE.HOUR), "hour"),
                to   = floor_date(max(CRDS_20250925_20250930_hourly$DATE.HOUR), "hour"),
                by   = "hour"),
        analyzer = unique(CRDS_20250925_20250930_hourly$analyzer),
        location = unique(CRDS_20250925_20250930_hourly$location),
        lab      = unique(CRDS_20250925_20250930_hourly$lab)) %>%
        mutate(across(c(location, analyzer, lab), as.character))


# Step 3: Join to insert NA rows for missing hour × analyzer × lab
CRDS_20250925_20250930_hourly_final <- CRDS_20250925_20250930_dummy %>%
        left_join(CRDS_20250925_20250930_hourly,
                  by = c("DATE.HOUR", "location", "analyzer", "lab"))


write_excel_csv(CRDS_20250925_20250930_hourly_final, "H_CRDS9_20250925_20250930.csv")


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

# Calculate the hourly average of all MPVPosition
CRDS_in_out <- CRDS_combined_df %>%
        filter(location %in% c("in","out")) %>%
        mutate(
                DATE.TIME = if_else(location == "out",
                                    DATE.TIME - seconds(450),  # shift out backwards
                                    DATE.TIME),
                DATE.HOUR = floor_date(DATE.TIME, "hour")
        ) %>%
        select(-step_id, -MPVPosition, -measuring.time) %>%
        group_by(DATE.HOUR, location) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"
        ) %>%
        tidyr::pivot_wider(
                names_from = location,
                values_from = c(CO2, CH4, NH3, H2O),
                names_sep = "_"
        )

write_excel_csv(CRDS_in_out, "H_CRDS_20250912-20250922.csv")