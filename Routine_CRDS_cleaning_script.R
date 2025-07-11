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

####### ATB Data importing and cleaning ########
#Picarro G2508
ATB_CRDS.1 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509/2025",
                      
                      output_path = "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean",
                      
                      result_file_name = "2025_Apr_Jun_7.5_ATB_CRDS.1",
                      
                      gas = c("CO2", "CH4", "NH3", "H2O", "N2O"),
                      
                      start_time = "2025-04-08 12:00:00",
                      
                      end_time = "2025-06-29 12:59:59",
                      
                      flush = 180, # Flush time in seconds
                      
                      interval = 450) # Total time at MPVPosition in seconds


# Read in the data
ATB_CRDS.1 <- read.csv("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2025_Apr_Jun_7.5_ATB_CRDS.1.csv")

# Create an hourly timestamp to group by
ATB_CRDS.1$DATE.TIME <- as.POSIXct(ATB_CRDS.1$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")
ATB_CRDS.1$DATE.TIME <- floor_date(ATB_CRDS.1$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
ATB_avg <- ATB_CRDS.1 %>%
        group_by(DATE.TIME, MPVPosition) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                N2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )

ATB_avg <- ATB_avg %>%
        filter(MPVPosition %in% c(1, 2, 3)) %>%
        mutate(
                location = recode(as.factor(MPVPosition),
                                  `1` = "N",
                                  `2` = "in",
                                  `3` = "S"),
                lab = factor("ATB"),
                analyzer = factor("CRDS.1")
        )

# Write long csv
ATB_avg <- ATB_avg %>% select(DATE.TIME, MPVPosition, location, lab, analyzer, everything())
ATB_avg$DATE.TIME <- format(ATB_avg$DATE.TIME, "%Y-%m-%d %H:%M:%S")
write.csv(ATB_avg,"2025_Apr_May_Jun_hourly_ATB_CRDS.1.csv" , row.names = FALSE, quote = FALSE)

# Change pivot to wide
ATB_wide <- ATB_avg %>%
        select(-MPVPosition) %>% 
        pivot_wider(
                names_from = location,
                values_from = c(CO2, CH4, NH3, H2O, N2O),
                names_sep = "_"
        )

# Write wide csv
write.csv(ATB_wide,"2025.04.08-2025.06.30_ATB_CRDS09_Gas_in_out.csv" , row.names = FALSE, quote = FALSE)

#For_LUFA experiments
# Define the time range
start_time <- as.POSIXct("2025-05-12 00:00:00")
end_time   <- as.POSIXct("2025-06-07 12:59:59")

# Filter and convert DATE.TIME
For_LUFA <- ATB_avg %>% 
        mutate(DATE.TIME = as.POSIXct(DATE.TIME, format = "%Y-%m-%d %H:%M:%S")) %>%
        filter(DATE.TIME >= start_time & DATE.TIME <= end_time)

For_LUFA$DATE.TIME <- format(For_LUFA$DATE.TIME, "%Y-%m-%d %H:%M:%S")
write.csv(For_LUFA,"2025.05.12-2025.06.07_ATB_hour_CRDS.1.csv" , row.names = FALSE, quote = FALSE)

