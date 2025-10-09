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

CRDS9_Sep_2025 <- read.csv("20250828.1200-20250925.1317_ATB_240avg_CRDS9.csv") %>%
        mutate(location = as.character(location)) %>%
        select("DATE.TIME", "analyzer", "location", "CO2", "CH4", "NH3", "H2O")

CRDS8_Sep_2025 <- read.csv("20250828.1200-20250925.1317_ATB_240avg_CRDS8.csv") %>%
        mutate(location = as.character(location)) %>%
        select("DATE.TIME", "analyzer", "location", "CO2", "CH4", "NH3", "H2O")


# Make location column consistent across all datasets
GAS_Jun_2024 <- GAS_Jun_2024 %>% mutate(location = as.character(location))
GAS_Jul_2024 <- GAS_Jul_2024 %>% mutate(location = as.character(location))
CRDS9_Sep_2025 <- CRDS9_Sep_2025 %>% mutate(location = as.character(location))
CRDS8_Sep_2025 <- CRDS8_Sep_2025 %>% mutate(location = as.character(location))

# Combine
GAS_combined <- bind_rows(GAS_Jun_2024, GAS_Jul_2024, CRDS9_Sep_2025, CRDS8_Sep_2025) %>%
        arrange(DATE.TIME) %>%
        mutate(analyzer = ifelse(analyzer == "FTIR2" & location == 52, "out", analyzer))

Sep_2025_loc_avg <- Sep_2025_data %>%group_by(location, MPVPosition) %>%
        summarise(CO2 = mean(CO2, na.rm = TRUE),
                  CH4 = mean(CH4, na.rm = TRUE),
                  NH3 = mean(NH3, na.rm = TRUE)) %>% ungroup()
