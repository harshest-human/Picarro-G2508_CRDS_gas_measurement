getwd()
####### libraries ######
library(tidyverse)
library(reshape2)
library(hablar)
library(lubridate)
library(psych)
library(ggplot2)
library(gganimate)
library(readxl)
library(dplyr)
library(ggpubr)
library(readr)
library(data.table)
source("Picarro_CRDS_data_cleaning_script.R")

####### Import data ######
GAS_Jun_2024 <- read.csv("D:/Data Analysis/Gas-Concentration-Analysis_time-series/2024_results/2024_June_GAS.long.csv") %>%
        mutate(analyzer = case_when(
                ID == "MPVPosition.P9" ~ "CRDS9",
                ID == "MPVPosition.P8" ~ "CRDS8",
                ID == "Messstelle.F2"  ~ "FTIR2",
                ID == "Messstelle.F1"  ~ "FTIR1",
                TRUE ~ "unknown")) %>%
        rename(location = sampling.point) %>%
        select(-ID) %>%
        relocate(analyzer, .after = DATE.TIME)

GAS_Jul_2024 <- read.csv("D:/Data Analysis/Gas-Concentration-Analysis_time-series/2024_results/2024_July_GAS.long.csv") %>%
        mutate(analyzer = case_when(
                ID == "MPVPosition.P9" ~ "CRDS9",
                ID == "MPVPosition.P8" ~ "CRDS8",
                ID == "Messstelle.F2"  ~ "FTIR2",
                ID == "Messstelle.F1"  ~ "FTIR1",
                TRUE ~ "unknown")) %>%
        rename(location = sampling.point) %>%
        select(-ID) %>%
        relocate(analyzer, .after = DATE.TIME)

CRDS9_Aug_2025 <- read.csv("20250828.1200-20250925.1317_ATB_240avg_CRDS9.csv") %>%
        mutate(location = as.character(location)) %>%
        select("DATE.TIME", "analyzer", "location", "CO2", "CH4", "NH3", "H2O")

CRDS8_Aug_2025 <- read.csv("20250828.1200-20250925.1317_ATB_240avg_CRDS8.csv") %>%
        mutate(location = as.character(location)) %>%
        select("DATE.TIME", "analyzer", "location", "CO2", "CH4", "NH3", "H2O")

CRDS9_Sep_2025 <- read.csv("20250925.1213-20250930.2330_ATB_240avg_CRDS9.csv") %>%
        mutate(location = as.character(location)) %>%
        select("DATE.TIME", "analyzer", "location", "CO2", "CH4", "NH3", "H2O")

CRDS8_Sep_2025 <- read.csv("20250925.1206-20250930.2319_ATB_240avg_CRDS8.csv") %>%
        mutate(location = as.character(location)) %>%
        select("DATE.TIME", "analyzer", "location", "CO2", "CH4", "NH3", "H2O")

CRDS9_Oct_2025 <- read.csv("20250930.2330-20251010.0926_ATB_240avg_CRDS9.csv") %>%
        mutate(location = as.character(location)) %>%
        select("DATE.TIME", "analyzer", "location", "CO2", "CH4", "NH3", "H2O")

CRDS8_Oct_2025 <- read.csv("20250930.2319-20251010.0923_ATB_240avg_CRDS8.csv") %>%
        mutate(location = as.character(location)) %>%
        select("DATE.TIME", "analyzer", "location", "CO2", "CH4", "NH3", "H2O")


############# DATA PROCESSING ##########
# Make all location columns character
GAS_Jun_2024$location <- as.character(GAS_Jun_2024$location)
GAS_Jul_2024$location <- as.character(GAS_Jul_2024$location)
CRDS8_Aug_2025$location <- as.character(CRDS8_Aug_2025$location)
CRDS9_Aug_2025$location <- as.character(CRDS9_Aug_2025$location)
CRDS8_Sep_2025$location <- as.character(CRDS8_Sep_2025$location)
CRDS9_Sep_2025$location <- as.character(CRDS9_Sep_2025$location)
CRDS8_Oct_2025$location <- as.character(CRDS8_Oct_2025$location)
CRDS9_Oct_2025$location <- as.character(CRDS9_Oct_2025$location)

# Bind rows
GAS_combined <- bind_rows(
        GAS_Jun_2024, GAS_Jul_2024,
        CRDS8_Aug_2025, CRDS9_Aug_2025,
        CRDS8_Sep_2025, CRDS9_Sep_2025,
        CRDS8_Oct_2025, CRDS9_Oct_2025) %>%
        mutate(DATE.TIME = as.POSIXct(DATE.TIME)) %>%
        filter(!is.na(location) & location != "") %>%
        mutate(location = ifelse(location == "in", "53", location),
               location = ifelse(location == "S", "52", location),
               location = trimws(location)) %>%
        mutate(location = factor(location,
                                 levels = as.character(sort
                                                       (as.numeric(unique(location)))))) %>%
        droplevels() %>%
        arrange(as.numeric(as.character(location))) 

anylzer_location <- GAS_combined %>%
        group_by(location) %>%
        summarise(
                n_analyzers = n_distinct(analyzer),
                analyzers = paste(sort(unique(analyzer)), collapse = ", ")
        )

# Create vertcial group list
vertical_groups <- list(top = c(1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31, 34, 37, 40, 43, 46, 49),
                        mid = c(2, 5, 8, 11, 14, 17, 20, 23, 26, 29, 32, 35, 38, 41, 44, 47, 50),
                        bottom = c(3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 48, 51),
                        ref.ring = 53,
                        out = 52) 

# Calculate ratios
GAS_ratios <- GAS_combined %>% group_by(DATE.TIME, analyzer, location) %>%
        mutate(CHCO = as.numeric((CH4/CO2)*100, na.rm = TRUE),
                  NHCO = as.numeric((NH3/CO2)*100, na.rm = TRUE)) %>% ungroup() %>%
        arrange(location) %>%
        mutate(vgroup = case_when(
                location %in% vertical_groups$top    ~ "top",
                location %in% vertical_groups$mid    ~ "mid",
                location %in% vertical_groups$bottom ~ "bottom",
                location == vertical_groups$`ref.ring`  ~ "ref.ring",
                location == vertical_groups$out          ~ "out"))


# Write csv
write_excel_csv(GAS_ratios, "20251010_high_resolution_gas_concentration_data.csv")


