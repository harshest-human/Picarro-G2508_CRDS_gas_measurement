getwd()
####### libraries ######
library(tidyverse)
library(reshape2)
library(hablar)
library(lubridate)
library(psych)
library(ggplot2)
library(readxl)
library(openxlsx)
library(writexl)
library(dplyr)
library(ggpubr)
library(readr)
library(data.table)
source("Picarro_CRDS_data_cleaning_script.R")

####### 2026-02-17 to 2026-02-23 ATB Data importing and cleaning ########
CRDS8_20260217_20260223 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2026/02",
                                   
                                   gas = c("CO2", "CH4", "NH3", "H2O", "N2O"),
                                   
                                   start_time = "2026-02-17 13:00:00",
                                   
                                   end_time = "2026-02-23 10:45:17",
                                   
                                   flush = 60, # Flush time in seconds
                                   
                                   interval = 240,  # Total time at MPVPosition in seconds
                                   
                                   MPVPosition.levels = c("1", "2", "3", "4", "5", "6", "7", "8","9"),
                                   
                                   location.levels = c("1", "2", "3", "4", "5", "6", "7", "in", "S"),
                                   
                                   lab = "ATB",
                                   
                                   analyzer = "CRDS8")


reshape_crds <- function(df_raw) {
        
        valid_locations <- c("in", "S", "N")
        
        df_hourly_long <- df_raw %>%
                filter(location %in% valid_locations) %>%
                mutate(DATE.HOUR = floor_date(DATE.TIME, "hour")) %>%
                group_by(DATE.HOUR, location, lab, analyzer) %>%
                summarise(across(c(CO2, CH4, NH3, H2O, N2O), ~mean(.x, na.rm = TRUE)),
                          .groups = "drop")
        
        df_dummy <- expand_grid(
                DATE.HOUR = seq(min(df_hourly_long$DATE.HOUR),
                                max(df_hourly_long$DATE.HOUR),
                                by = "hour"),
                analyzer = unique(df_hourly_long$analyzer),
                location = unique(df_hourly_long$location),
                lab      = unique(df_hourly_long$lab)
        )
        
        df_hourly_wide <- df_dummy %>%
                left_join(df_hourly_long,
                          by = c("DATE.HOUR", "location", "analyzer", "lab")) %>%
                pivot_wider(
                        names_from = location,
                        values_from = c(CO2, CH4, NH3, H2O, N2O),
                        names_sep = "_"
                ) %>%
                arrange(DATE.HOUR)
        
        make_final <- function(df_wide, suffix, site_code_label) {
                
                gases <- c("CO2", "CH4", "NH3", "H2O", "N2O")
                cols <- paste0(gases, "_", suffix)
                cols <- cols[cols %in% names(df_wide)]
                
                df_site <- df_wide %>%
                        select(DATE.HOUR, analyzer, lab, all_of(cols)) %>%
                        rename_with(~ gsub(paste0("_", suffix), "", .x), all_of(cols))
                
                for (g in gases) {
                        if (!g %in% names(df_site)) df_site[[g]] <- NA_real_
                }
                
                df_out <- df_site %>%
                        mutate(
                                LOCATION = "Gross Kreutz",
                                SITE_CODE = site_code_label,
                                MEASUREMENT_PERIOD = "",
                                DATE_TIME = DATE.HOUR,
                                MEASUREMENT_POINT_CODE = lab,
                                GAS_MEASUREMENT_METHOD = paste0("LINE_", analyzer),
                                CO2_DRY_PPM = CO2,
                                CH4_DRY_PPM = CH4,
                                NH3_DRY_PPM = NH3,
                                H2O_VOL_PCT = H2O,
                                N2O_DRY_PPM = N2O,
                                NO2_DRY_PPM = "",
                                NO_DRY_PPM = "",
                                CO_DRY_PPM = "",
                                COMMENTS = "",
                                AVG_TOTAL_NUMBER_ANIMALS = "",
                                AVG_WEIGHT_KG = "",
                                AVG_DAYS_IN_PREGNANCY = "",
                                AVG_MILK_YIELD_CORR_KGAD = "",
                                IM_AIR_TEMPERATURE_C = ""
                        ) %>%
                        select(
                                LOCATION, SITE_CODE, MEASUREMENT_PERIOD, DATE_TIME,
                                MEASUREMENT_POINT_CODE, GAS_MEASUREMENT_METHOD,
                                CO2_DRY_PPM, CH4_DRY_PPM, NH3_DRY_PPM, H2O_VOL_PCT,
                                N2O_DRY_PPM, NO2_DRY_PPM, NO_DRY_PPM, CO_DRY_PPM,
                                COMMENTS, AVG_TOTAL_NUMBER_ANIMALS, AVG_WEIGHT_KG,
                                AVG_DAYS_IN_PREGNANCY, AVG_MILK_YIELD_CORR_KGAD,
                                IM_AIR_TEMPERATURE_C
                        ) %>%
                        arrange(DATE_TIME)
                
                attr(df_out, "start_dt") <- min(df_out$DATE_TIME, na.rm = TRUE)
                attr(df_out, "end_dt")   <- max(df_out$DATE_TIME, na.rm = TRUE)
                
                return(df_out)
        }
        
        return(list(
                hourly_wide = df_hourly_wide,
                final_IN = make_final(df_hourly_wide, "in", "IN"),
                final_OUT_South = make_final(df_hourly_wide, "S", "OUT_South"),
                final_OUT_North = make_final(df_hourly_wide, "N", "OUT_North")
        ))
}

CRDS8_20260217_20260223_multi <- reshape_crds(CRDS8_20260217_20260223)

# Write the wide CSV
write_csv(CRDS8_20260217_20260223_multi$hourly_wide, "H_CRDS8+9_20260217_20260223.csv")

# Write the final Excel files
write_xlsx(CRDS8_20260217_20260223_multi$final_IN,        "GrossKreutz_IN_2026-02-17_to_2026-02-23.xlsx")
write_xlsx(CRDS8_20260217_20260223_multi$final_OUT_South, "GrossKreutz_OUT_South_2026-02-17_to_2026-02-23.xlsx")
write_xlsx(CRDS8_20260217_20260223_multi$final_OUT_North, "GrossKreutz_OUT_North_2026-02-17_to_2026-02-23.xlsx")

####### 2026-02-23 to 2026-03-02 ATB Data importing and cleaning ########
CRDS8_20260223_20260302 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2026",
                                   
                                   gas = c("CO2", "CH4", "NH3", "H2O", "N2O"),
                                   
                                   start_time = "2026-02-23 10:45:17",
                                   
                                   end_time = "2026-03-02 08:58:12",
                                   
                                   flush = 60, # Flush time in seconds
                                   
                                   interval = 240,  # Total time at MPVPosition in seconds
                                   
                                   MPVPosition.levels = c("1", "2", "3", "4", "5", "6", "7", "8","9"),
                                   
                                   location.levels = c("1", "2", "3", "4", "5", "6", "7", "in", "S"),
                                   
                                   lab = "ATB",
                                   
                                   analyzer = "CRDS8")


reshape_crds <- function(df_raw) {
        
        valid_locations <- c("in", "S", "N")
        
        df_hourly_long <- df_raw %>%
                filter(location %in% valid_locations) %>%
                mutate(DATE.HOUR = floor_date(DATE.TIME, "hour")) %>%
                group_by(DATE.HOUR, location, lab, analyzer) %>%
                summarise(across(c(CO2, CH4, NH3, H2O, N2O), ~mean(.x, na.rm = TRUE)),
                          .groups = "drop")
        
        df_dummy <- expand_grid(
                DATE.HOUR = seq(min(df_hourly_long$DATE.HOUR),
                                max(df_hourly_long$DATE.HOUR),
                                by = "hour"),
                analyzer = unique(df_hourly_long$analyzer),
                location = unique(df_hourly_long$location),
                lab      = unique(df_hourly_long$lab)
        )
        
        df_hourly_wide <- df_dummy %>%
                left_join(df_hourly_long,
                          by = c("DATE.HOUR", "location", "analyzer", "lab")) %>%
                pivot_wider(
                        names_from = location,
                        values_from = c(CO2, CH4, NH3, H2O, N2O),
                        names_sep = "_"
                ) %>%
                arrange(DATE.HOUR)
        
        make_final <- function(df_wide, suffix, site_code_label) {
                
                gases <- c("CO2", "CH4", "NH3", "H2O", "N2O")
                cols <- paste0(gases, "_", suffix)
                cols <- cols[cols %in% names(df_wide)]
                
                df_site <- df_wide %>%
                        select(DATE.HOUR, analyzer, lab, all_of(cols)) %>%
                        rename_with(~ gsub(paste0("_", suffix), "", .x), all_of(cols))
                
                for (g in gases) {
                        if (!g %in% names(df_site)) df_site[[g]] <- NA_real_
                }
                
                df_out <- df_site %>%
                        mutate(
                                LOCATION = "Gross Kreutz",
                                SITE_CODE = site_code_label,
                                MEASUREMENT_PERIOD = "",
                                DATE_TIME = DATE.HOUR,
                                MEASUREMENT_POINT_CODE = lab,
                                GAS_MEASUREMENT_METHOD = paste0("LINE_", analyzer),
                                CO2_DRY_PPM = CO2,
                                CH4_DRY_PPM = CH4,
                                NH3_DRY_PPM = NH3,
                                H2O_VOL_PCT = H2O,
                                N2O_DRY_PPM = N2O,
                                NO2_DRY_PPM = "",
                                NO_DRY_PPM = "",
                                CO_DRY_PPM = "",
                                COMMENTS = "",
                                AVG_TOTAL_NUMBER_ANIMALS = "",
                                AVG_WEIGHT_KG = "",
                                AVG_DAYS_IN_PREGNANCY = "",
                                AVG_MILK_YIELD_CORR_KGAD = "",
                                IM_AIR_TEMPERATURE_C = ""
                        ) %>%
                        select(
                                LOCATION, SITE_CODE, MEASUREMENT_PERIOD, DATE_TIME,
                                MEASUREMENT_POINT_CODE, GAS_MEASUREMENT_METHOD,
                                CO2_DRY_PPM, CH4_DRY_PPM, NH3_DRY_PPM, H2O_VOL_PCT,
                                N2O_DRY_PPM, NO2_DRY_PPM, NO_DRY_PPM, CO_DRY_PPM,
                                COMMENTS, AVG_TOTAL_NUMBER_ANIMALS, AVG_WEIGHT_KG,
                                AVG_DAYS_IN_PREGNANCY, AVG_MILK_YIELD_CORR_KGAD,
                                IM_AIR_TEMPERATURE_C
                        ) %>%
                        arrange(DATE_TIME)
                
                attr(df_out, "start_dt") <- min(df_out$DATE_TIME, na.rm = TRUE)
                attr(df_out, "end_dt")   <- max(df_out$DATE_TIME, na.rm = TRUE)
                
                return(df_out)
        }
        
        return(list(
                hourly_wide = df_hourly_wide,
                final_IN = make_final(df_hourly_wide, "in", "IN"),
                final_OUT_South = make_final(df_hourly_wide, "S", "OUT_South"),
                final_OUT_North = make_final(df_hourly_wide, "N", "OUT_North")
        ))
}

CRDS8_20260223_20260302_multi <- reshape_crds(CRDS8_20260223_20260302)

# Write the wide CSV
write_csv(CRDS8_20260223_20260302_multi$hourly_wide, "H_CRDS8+9_20260223_20260302.csv")

# Write the final Excel files
write_xlsx(CRDS8_20260223_20260302_multi$final_IN,        "GrossKreutz_IN_2026-02-23_to_2026-03-02.xlsx")
write_xlsx(CRDS8_20260223_20260302_multi$final_OUT_South, "GrossKreutz_OUT_South_2026-02-23_to_2026-03-02.xlsx")
write_xlsx(CRDS8_20260223_20260302_multi$final_OUT_North, "GrossKreutz_OUT_North_2026-02-23_to_2026-03-02.xlsx")


####### 2026-03-02 to 2026-03-09 ATB Data importing and cleaning ########
CRDS8_20260302_20260309 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2026",
                                   
                                   gas = c("CO2", "CH4", "NH3", "H2O", "N2O"),
                                   
                                   start_time = "2026-03-02 07:58:12",
                                   
                                   end_time = "2026-03-09 08:35:10",
                                   
                                   flush = 60, # Flush time in seconds
                                   
                                   interval = 240,  # Total time at MPVPosition in seconds
                                   
                                   MPVPosition.levels = c("1", "2", "3", "4", "5", "6", "7", "8","9"),
                                   
                                   location.levels = c("1", "2", "3", "4", "5", "6", "7", "in", "S"),
                                   
                                   lab = "ATB",
                                   
                                   analyzer = "CRDS8")


CRDS8_20260302_20260309_multi <- reshape_crds(CRDS8_20260302_20260309)

# Write the wide CSV
write_csv(CRDS8_20260302_20260309_multi$hourly_wide, "H_CRDS8+9_20260302_20260309.csv")

# Write the final Excel files
write_xlsx(CRDS8_20260302_20260309_multi$final_IN,        "GrossKreutz_IN_2026-03-02_to_2026-03-09.xlsx")
write_xlsx(CRDS8_20260302_20260309_multi$final_OUT_South, "GrossKreutz_OUT_South_2026-03-02_to_2026-03-09.xlsx")
write_xlsx(CRDS8_20260302_20260309_multi$final_OUT_North, "GrossKreutz_OUT_North_2026-03-02_to_2026-03-09.xlsx")


