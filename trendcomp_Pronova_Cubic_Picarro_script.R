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
source("remove_outliers_function.R")

####### Import Picarro dataset #######
H_CRDS_20250828_20250925 <- read.csv("H_CRDS8+9_20250828_20250925.csv")
H_CRDS_20250925_20250930 <- read.csv("H_CRDS8+9_20250925_20250930.csv")
H_CRDS_20250930_20251010 <- read.csv("H_CRDS8+9_20250930_20251010.csv")
H_CRDS_20251010_20251024 <- read.csv("H_CRDS8+9_20251010_20251024.csv")
H_CRDS_20251024_20251030 <- read.csv("H_CRDS8+9_20251024_20251030.csv") 
H_CRDS_20251101_20251112 <- read.csv("H_CRDS8+9_20251101_20251112.csv") 
H_CRDS_20251112_20251114 <- read.csv("H_CRDS8+9_20251112_20251114.csv") 
H_CRDS_20251209_20251231 <- read.csv("H_CRDS8+9_20251209_20251231.csv") 
H_CRDS_20260101_20260119 <- read.csv("H_CRDS8+9_20260101_20260119.csv")

# Combine all into one data frame
CRDS_data <- bind_rows(
        H_CRDS_20250828_20250925,
        H_CRDS_20250925_20250930,
        H_CRDS_20250930_20251010,
        H_CRDS_20251010_20251024,
        H_CRDS_20251024_20251030,
        H_CRDS_20251101_20251112,
        H_CRDS_20251112_20251114,
        H_CRDS_20251209_20251231,
        H_CRDS_20260101_20260119) %>%
        mutate(DATE.HOUR = ymd_hms(DATE.HOUR),        
               delta_CO2 = CO2_in - CO2_S,       
               delta_CH4 = CH4_in - CH4_S,
               delta_NH3 = NH3_in - NH3_S,
               delta_N2O = N2O_in - N2O_S) %>%
        select(DATE.HOUR, delta_CO2, delta_CH4, delta_NH3, delta_N2O,analyzer)


####### Import Pronova dataset #######
#  Set folder path
pronova_path <- "D:/Data Analysis/Gas_data/Raw_data/Pronova Data/Messdaten"

# List all logged data files
pronova_files <- list.files(
        path = pronova_path,
        pattern = "^differenzmessung_.*\\.txt$",
        full.names = TRUE
)

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
Pronova_data  <- pronova_files %>%
        lapply(pronova_read_log_file) %>%
        bind_rows() %>%
        mutate(DATE.TIME = dmy_hms(`Datum Uhrzeit`)) %>%
        mutate(DATE.HOUR = floor_date(DATE.TIME, "hour")) %>%
        group_by(DATE.HOUR) %>%
        summarise(delta_CH4 = mean(as.numeric(gsub(",", ".", `CH4 in ppm`)), na.rm = TRUE),
                  delta_CO2 = mean(as.numeric(gsub(",", ".", `CO2 in ppm`)), na.rm = TRUE),
                  delta_N2O = mean(as.numeric(gsub(",", ".", `N2O in ppm`)), na.rm = TRUE),
                  delta_NH3 = mean(as.numeric(gsub(",", ".", `NH3 in ppm`)), na.rm = TRUE)) %>%
        mutate(analyzer = "Pronova")

####### Import Cubic dataset #########
# 1. Set the directory path
cubic_path <- "D:/Data Analysis/Gas_data/Raw_data/Cubic_raw"

# 2. List all .xlsx files
files <- list.files(path = cubic_path, 
                    pattern = "^[^~].*\\.xlsx$", 
                    full.names = TRUE)

# 3. Read all files and bind them row-wise
Cubic_data <- files %>%
        map_df(~ read_excel(.x)) %>%
        mutate(Time = ymd_hms(Time),
               DATE.HOUR = floor_date(Time, "hour")) %>%
        pivot_longer(cols = any_of(c("CH4", "NH3", "CO2")),
                     names_to = "gas", values_to = "value") %>%
        mutate(gas_name = paste0(gas, ifelse(Type == 1, "_in", "_S"))) %>%
        group_by(DATE.HOUR, gas_name) %>%
        summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%  # aggregate duplicates
        pivot_wider(names_from = gas_name, values_from = value) %>%
        arrange(DATE.HOUR) %>%
        mutate(DATE.HOUR = as.POSIXct(DATE.HOUR),        
               delta_CO2 = CO2_in - CO2_S,       
               delta_CH4 = CH4_in - CH4_S,
               delta_NH3 = NH3_in - NH3_S,
               analyzer = "Cubic") %>%
        select(DATE.HOUR, delta_CO2, delta_CH4, delta_NH3, analyzer)

####### Combine Picarro, Pronova, and Cubic #######
Gas_data <- bind_rows(Pronova_data,CRDS_data, Cubic_data) %>% arrange(DATE.HOUR)
Gas_data <- remove_outliers(Gas_data, exclude_cols = c("DATE.HOUR", "analyzer"), group_cols = NULL)

####### Data visulation #######
trendcomp <- function(data, gas = "CO2", start_date, end_date, month_label, date_breaks = "2 day") {
        
        # Define colors: darker for CRDS, lighter for Pronova, add Cubic colors
        gas_colors <- list(
                CO2 = c(
                        "CRDS9"  = "#1f78b4",
                        "CRDS8"  = "#1f78b4",
                        "Pronova"= "#a6cee3",
                        "Cubic"  = "#6a3d9a" 
                ),
                CH4 = c(
                        "CRDS9"  = "#33a02c",
                        "CRDS8"  = "#33a02c",
                        "Pronova"= "#b2df8a",
                        "Cubic"  = "#cab2d6"
                ),
                NH3 = c(
                        "CRDS9"  = "#ff7f00",
                        "CRDS8"  = "#ff7f00",
                        "Pronova"= "#fdbf6f",
                        "Cubic"  = "#fb9a99"
                ),
                N2O = c(
                        "CRDS9"  = "#e31a1c",
                        "CRDS8"  = "#e31a1c",
                        "Pronova"= "#fb9a99",
                        "Cubic"  = "#b15928"
                )
        )
        
        # Select correct column dynamically
        gas_column <- paste0("delta_", gas)
        
        df <- data %>%
                filter(DATE.HOUR >= start_date,
                       DATE.HOUR <= end_date) %>%
                filter(!is.na(.data[[gas_column]]))
        
        # Trim whitespace from analyzer names
        df$analyzer <- trimws(df$analyzer)
        
        # Only keep colors for analyzers actually present
        available_levels <- unique(df$analyzer)
        available_colors <- gas_colors[[gas]][available_levels]
        
        y_labels <- list(
                CO2 = expression(Delta~CO[2]~"(ppm)"),
                CH4 = expression(Delta~CH[4]~"(ppm)"),
                NH3 = expression(Delta~NH[3]~"(ppm)"),
                N2O = expression(Delta~N[2]*O~"(ppm)")
        )
        
        # Plot
        p <- ggplot(df, aes(DATE.HOUR, .data[[gas_column]], color = analyzer)) +
                geom_line(size = 0.7) +
                geom_point(size = 1.4, alpha = 0.75) +
                scale_color_manual(values = available_colors) +
                scale_x_datetime(date_breaks = date_breaks, date_labels = "%d-%b") +
                labs(title = paste(month_label, "- Time Series of Delta", gas),
                     x = "Date",
                     y = y_labels[[gas]],
                     color = "Analyzer") +
                theme_minimal(base_size = 12) +
                theme(axis.text.x = element_text(angle = 45, hjust = 1),
                      legend.position = "bottom",
                      plot.title = element_text(face = "bold"))
        
        return(p)
}

# October
trendcomp(Gas_data, "CO2",
          "2025-10-01 00:00:00",
          "2025-10-30 23:00:00",
          "October")

trendcomp(Gas_data, "CH4",
          "2025-10-01 00:00:00",
          "2025-10-30 23:00:00",
          "October")

trendcomp(Gas_data, "NH3",
          "2025-10-01 00:00:00",
          "2025-10-30 23:00:00",
          "October")

# November
trendcomp(Gas_data, "CO2",
          "2025-11-01 00:00:00",
          "2025-11-30 23:00:00",
          "November")

trendcomp(Gas_data, "CH4",
          "2025-11-01 00:00:00",
          "2025-11-30 23:00:00",
          "November")

trendcomp(Gas_data, "NH3",
          "2025-11-01 00:00:00",
          "2025-11-30 23:00:00",
          "November")

# December
trendcomp(Gas_data, "CO2",
          "2025-12-01 00:00:00",
          "2025-12-31 23:00:00",
          "December")

trendcomp(Gas_data, "CH4",
          "2025-12-01 00:00:00",
          "2025-12-31 23:00:00",
          "December")

trendcomp(Gas_data, "NH3",
          "2025-12-01 00:00:00",
          "2025-12-31 23:00:00",
          "December")

# January
trendcomp(Gas_data, "CO2",
          "2026-01-01 00:00:00",
          "2026-01-31 23:00:00",
          "January")

trendcomp(Gas_data, "CH4",
          "2026-01-01 00:00:00",
          "2026-01-31 23:00:00",
          "January")

trendcomp(Gas_data, "NH3",
          "2026-01-01 00:00:00",
          "2026-01-31 23:00:00",
          "January")

# Set a folder to save plots (optional)
plot_dir <- "Picarro_vs_Pronova_vs_Cubic_Plots"
if(!dir.exists(plot_dir)) dir.create(plot_dir)

months <- list(
        October  = c("2025-10-01 00:00:00", "2025-10-24 23:00:00"),
        November = c("2025-11-01 00:00:00", "2025-11-30 23:00:00"),
        December = c("2025-12-01 00:00:00", "2025-12-31 23:00:00"),
        January  = c("2026-01-01 00:00:00", "2026-01-31 23:00:00")
)

gases <- c("CO2", "CH4", "NH3")

for (m in names(months)) {
        for (g in gases) {
                
                p <- trendcomp(Gas_data,
                               gas = g,
                               start_date = months[[m]][1],
                               end_date   = months[[m]][2],
                               month_label = m)
                
                filename <- paste0("Delta_", g, "_", m, ".png")
                
                ggsave(filename = file.path(plot_dir, filename),
                       plot = p,
                       width = 10,
                       height = 5,
                       dpi = 300)
        }
}


