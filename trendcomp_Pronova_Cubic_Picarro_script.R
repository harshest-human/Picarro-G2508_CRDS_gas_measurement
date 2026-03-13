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
library(scales)
library(purrr)
source("remove_outliers_function.R")
source("indirect.CO2.balance function.R")

####### Import Picarro dataset #######
# List all crds files
crds_files <- list.files("crds_data/crds_hour",
                                       pattern = "\\.csv$",
                                       full.names = TRUE,
                                       recursive = TRUE)

                                       
# Read, Filter Date Time and Calculate Delta
crds_data <- purrr::map_dfr(crds_files,read.csv) %>% 
        mutate(DATE.HOUR = ymd_hms(DATE.HOUR),        
               delta_CO2 = CO2_in - CO2_S,       
               delta_CH4 = CH4_in - CH4_S,
               delta_NH3 = NH3_in - NH3_S,
               analyzer = "Picarro") %>%
        filter(DATE.HOUR >= "2025-10-01 00:00:00", DATE.HOUR <= "2026-02-23 23:00:00") %>%
        select(DATE.HOUR, analyzer, delta_CO2, delta_CH4, delta_NH3) %>% 
        remove_outliers(exclude_cols = c("DATE.HOUR", "analyzer"),
                        group_cols = c("DATE.HOUR"))


####### Import Pronova dataset #######
# List all pronova files
pronova_files <- list.files(
        path = "pronova_data/Messdaten",
        pattern = "^differenzmessung_.*\\.txt$",
        full.names = TRUE)

# Function to safely read one file
pronova_read_log_file <- function(file) {
        message("Reading: ", basename(file))
        df <- tryCatch(
                read.table(
                        file,
                        header = TRUE,
                        sep = "\t",
                        dec = ",",
                        check.names = FALSE,
                        stringsAsFactors = FALSE,
                        fill = TRUE,
                        comment.char = "",
                        colClasses = "character"   
                ),
                error = function(e) {
                        message("Error reading ", basename(file), ": ", e$message)
                        return(NULL)
                }
        )
        return(df)
}

# Read and combine all files safely
pronova_data  <- pronova_files %>%
        lapply(pronova_read_log_file) %>%
        bind_rows() %>%
        mutate(DATE.TIME = dmy_hms(`Datum Uhrzeit`)) %>%
        mutate(DATE.HOUR = floor_date(DATE.TIME, "hour")) %>%
        group_by(DATE.HOUR) %>%
        summarise(delta_CH4 = mean(as.numeric(gsub(",", ".", `CH4 in ppm`)), na.rm = TRUE),
                  delta_CO2 = mean(as.numeric(gsub(",", ".", `CO2 in ppm`)), na.rm = TRUE),
                  delta_N2O = mean(as.numeric(gsub(",", ".", `N2O in ppm`)), na.rm = TRUE),
                  delta_NH3 = mean(as.numeric(gsub(",", ".", `NH3 in ppm`)), na.rm = TRUE)) %>%
        mutate(analyzer = "Pronova") %>%
        filter(DATE.HOUR >= "2025-10-01 00:00:00", DATE.HOUR <= "2026-02-23 23:00:00") %>%
        select(DATE.HOUR, analyzer, delta_CO2, delta_CH4, delta_NH3) %>% 
        remove_outliers(exclude_cols = c("DATE.HOUR", "analyzer"),
                        group_cols = c("DATE.HOUR"))



####### Import Cubic dataset #########
# List all cubic files
cubic_files <- list.files(path = "cubic_data/cubic_raw", 
                    pattern = "^[^~].*\\.xlsx$", 
                    full.names = TRUE)

# Read all files and bind them row-wise
cubic_data <- cubic_files %>%
        map_df(~ read_excel(.x)) %>%
        mutate(Time = ymd_hms(Time),
               DATE.HOUR = floor_date(Time, "hour")) %>%
        pivot_longer(cols = any_of(c("CH4", "NH3", "CO2")),
                     names_to = "gas", values_to = "value") %>%
        mutate(gas_name = paste0(gas, ifelse(Type == 1, "_in", "_S"))) %>%
        group_by(DATE.HOUR, gas_name) %>%
        summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>% 
        pivot_wider(names_from = gas_name, values_from = value) %>%
        arrange(DATE.HOUR) %>%
        mutate(DATE.HOUR = as.POSIXct(DATE.HOUR),        
               delta_CO2 = CO2_in - CO2_S,       
               delta_CH4 = CH4_in - CH4_S,
               delta_NH3 = NH3_in - NH3_S,
               analyzer = "Cubic")  %>%
        filter(DATE.HOUR >= "2025-10-01 00:00:00", DATE.HOUR <= "2026-02-23 23:00:00") %>%
        select(DATE.HOUR, analyzer, delta_CO2, delta_CH4, delta_NH3) %>% 
        remove_outliers(exclude_cols = c("DATE.HOUR", "analyzer"),
                        group_cols = c("DATE.HOUR"))

####### Combine Picarro, Pronova, and Cubic #######
Gas_data <- bind_rows(pronova_data,crds_data, cubic_data) %>% arrange(DATE.HOUR)
Gas_data <- remove_outliers(Gas_data, exclude_cols = c("DATE.HOUR", "analyzer"), group_cols = NULL)

####### Calculate ventilation rate and Emissions ######
# Import animal data
animal_data <- read_excel("animal_data/animal_data_2025-01-10_2026-01-19.xlsx") 

# Import temperature data
temp_data <- list.files("weather_data/temp_rh",
                        pattern = "\\.csv$",
                        full.names = TRUE,
                        recursive = TRUE) %>%
        purrr::map_dfr(~ readr::read_csv(.x, show_col_types = FALSE)) %>%
        dplyr::select(Date, T_inside) %>%
        dplyr::rename(temp_in = T_inside) %>%
        dplyr::mutate(DATE.TIME = lubridate::mdy_hms(sub(" \\+0000$", "", Date)),
                      DATE.HOUR = lubridate::floor_date(DATE.TIME, "hour")) %>%
        dplyr::group_by(DATE.HOUR) %>%
        dplyr::summarise(temp_in = mean(temp_in, na.rm = TRUE),
                         .groups = "drop")

# Combine the Input data
input_data <- Gas_data %>%
        left_join(animal_data, by = "DATE.HOUR", relationship = "many-to-many") %>%
        left_join(temp_data,   by = "DATE.HOUR", relationship = "many-to-many") %>%
        filter(DATE.HOUR >= ymd_hms("2025-12-09 12:00:00"),
               DATE.HOUR <= ymd_hms("2025-12-24 23:00:00")) %>%
        rename("DATE.TIME" = "DATE.HOUR")

# Calculate Emissions
emission_data <- indirect.CO2.balance(input_data)

emission_reshaped <-reshaper(emission_data)

write_excel_csv(emission_reshaped, "emission_reshaped_20251209_20251224.csv")


####### Data Visualization ########
d_errorbarplot <- emierrorbarplot(emission_reshaped, y = c("delta_CO2", "delta_CH4", "delta_NH3"))

d_trendplot <- emitrendplot(emission_reshaped,
                            y = c("delta_CO2", "delta_CH4", "delta_NH3"))

q_e_errorbarplot <- emierrorbarplot(data = emission_reshaped,
                                    y = c("Q_vent", "e_CH4_ghLU", "e_NH3_ghLU"))

q_e_trend_plot <- emitrendplot(data = emission_reshaped,
                               y = c("Q_vent", "e_CH4_ghLU", "e_NH3_ghLU"))
