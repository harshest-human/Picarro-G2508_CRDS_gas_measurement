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
CRDS8_20250930_20251010 <- read.csv("20250930.2319-20251010.0923_ATB_240avg_CRDS8.csv")
CRDS8_20251010_20251024 <- read.csv("20251010.0923-20251024.1428_ATB_240avg_CRDS8.csv")
CRDS8_20251024_20251030 <- read.csv("20251024.1428-20251030.1506_ATB_240avg_CRDS8.csv")
CRDS8_20251030_20251101 <- read.csv("20251030.1506-20251101.0125_ATB_240avg_CRDS8.csv")
CRDS8_20251101_20251112 <- read.csv("20251101.0125-20251112.0155_ATB_240avg_CRDS8.csv")
CRDS8_20251112_20251114 <- read.csv("20251112.0155-20251114.0122_ATB_240avg_CRDS8.csv")
CRDS8_20251114_20251119 <- read.csv("20251114.0122-20251119.0640_ATB_240avg_CRDS8.csv")
CRDS8_20251209_20251231 <- read.csv("20251209.1200-20251231.2359_ATB_240avg_CRDS8.csv")

# Combine all into one data frame
CRDS_data <- bind_rows(
        CRDS8_20250930_20251010 %>% mutate(location = as.character(location)),
        CRDS8_20251010_20251024 %>% mutate(location = as.character(location)),
        CRDS8_20251024_20251030 %>% mutate(location = as.character(location)),
        CRDS8_20251030_20251101 %>% mutate(location = as.character(location)),
        CRDS8_20251101_20251112 %>% mutate(location = as.character(location)),
        CRDS8_20251112_20251114 %>% mutate(location = as.character(location)),
        CRDS8_20251114_20251119 %>% mutate(location = as.character(location)),
        CRDS8_20251209_20251231 %>% mutate(location = as.character(location))
) %>%
        mutate(
                DATE.HOUR = floor_date(ymd_hms(DATE.TIME, tz = "Europe/Berlin"), unit = "hour"),
                location = as.factor(location),
                analyzer = as.factor(analyzer)
        ) %>%
        group_by(DATE.HOUR, location, analyzer) %>%
        summarise(
                CO2_raw = mean(CO2, na.rm = TRUE),
                NH3_raw = mean(NH3, na.rm = TRUE),
                .groups = "drop"
        )

        
####### Import OTICE Dataset #######
OTICE_20250929_20251126 <- read.csv("D:/Data Analysis/Gas_data/Raw_data/OTICE_raw/2025/average_hourly_data_OTICE_20250929_20251126NEWCALIBRATION.csv")
OTICE_20251126_20251231 <- read.csv("D:/Data Analysis/Gas_data/Raw_data/OTICE_raw/2025/average_hourly_data_OTICE_20251126_20251231NEWCALIBRATION.csv")

# Combine all into one dataframe
OTICE_data <- bind_rows(
        OTICE_20250929_20251126,
        OTICE_20251126_20251231) %>%
        mutate(Datetime_Berlin = ifelse(
                nchar(Datetime_Berlin) == 10,
                paste0(Datetime_Berlin, " 00:00:00"),
                Datetime_Berlin),
               DATE.HOUR = ymd_hms(Datetime_Berlin, tz = "Europe/Berlin"),
                analyzer = as.factor(Type),
                location = as.factor(Node)) %>%
        rename(CO2_raw  = CO2.AVG_hourly,
               CO2_corr = CO2.AVG_barn_hourly,
               NH3_raw  = NH3_ppm_hourly,
               NH3_corr = NH3_ppm_barn_hourly) %>%
        select(-Datetime_Berlin, -Type, -Node)
                
####### Data visualization ######
# Step 1: Create weekly labels
OTICE_weekly <- OTICE_data %>%
        mutate(
                WeekLabel = paste0(month(DATE.HOUR, label = TRUE),
                                   " W", week(DATE.HOUR) - week(floor_date(DATE.HOUR, "month")) + 1)
        )

CRDS_weekly <- CRDS_data %>%
        mutate(
                WeekLabel = paste0(month(DATE.HOUR, label = TRUE),
                                   " W", week(DATE.HOUR) - week(floor_date(DATE.HOUR, "month")) + 1)
        )


# Step 2: Order WeekLabel chronologically
all_weeks <- sort(unique(c(OTICE_weekly$WeekLabel, CRDS_weekly$WeekLabel)))

OTICE_weekly <- OTICE_weekly %>%
        mutate(WeekLabel = factor(WeekLabel, levels = all_weeks))

CRDS_weekly <- CRDS_weekly %>%
        mutate(WeekLabel = factor(WeekLabel, levels = all_weeks))


# Create output folder
dir.create("OTICE_CRDS_Monthly_Plots", showWarnings = FALSE)

# Define months and gas variables
months_to_plot <- c(9, 10, 11, 12)
gas_vars <- c("CO2_raw", "NH3_raw")

for (m in months_to_plot) {
        
        OTICE_m <- OTICE_weekly %>%
                filter(month(DATE.HOUR) == m)
        
        CRDS_m <- CRDS_weekly %>%
                filter(month(DATE.HOUR) == m)
        
        month_name <- month.name[m]
        
        for (gas in gas_vars) {
                
                # Combine monthly datasets
                combined_m <- bind_rows(OTICE_m, CRDS_m)
                
                p <- ggplot(combined_m,
                            aes(x = DATE.HOUR,
                                y = .data[[gas]],
                                color = location,
                                linetype = analyzer,
                                group = interaction(location, analyzer))) +
                        geom_line() +
                        facet_wrap(~ WeekLabel, scales = "free_x") +
                        theme_minimal() +
                        labs(title = paste(month_name, "-", gas),
                             x = "Date",
                             y = paste0(gas, " (ppm)"),
                             color = "Location",
                             linetype = "Analyzer") +
                        scale_linetype_manual(values = c("solid", "dashed")) +
                        scale_x_datetime(date_labels = "%d-%b", date_breaks = "2 days") +
                        theme(
                                axis.text.x = element_text(angle = 45, hjust = 1),
                                strip.text = element_text(face = "bold")
                        )
                
                # 🔹 PRINT TO VIEWER
                print(p)
                
                # 🔹 SAVE AS PNG
                ggsave(
                        filename = paste0("OTICE_CRDS_Monthly_Plots/", month_name, "_", gas, ".png"),
                        plot = p,
                        width = 14,
                        height = 8,
                        dpi = 300
                )
        }
}

