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


####### Data importing and cleaning ########
#Picarro G2508 
input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2025/04"
output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
result_file_name <- "20250408_Ring_concatenated"
selected_columns <- c("DATE", "TIME", "MPVPosition", "CO2", "CH4", "NH3", "H2O")
CRDS.P8 <- piconcatenate(input_path, output_path, result_file_name, selected_columns)
CRDS.P8 <- fread("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/20250408_Ring_concatenated.csv")
CRDS.P8 <- CRDS.P8 %>% mutate(DATE.TIME = as.POSIXct(paste(DATE, TIME), format = "%Y-%m-%d %H:%M:%S"))
CRDS.P8 <- CRDS.P8 %>% filter(DATE.TIME >= as.POSIXct("2025-04-08 12:00:01"), DATE.TIME <= as.POSIXct("2025-04-09 12:00:00"))


CRDS.P8 <- CRDS.P8 %>%
        mutate(DATE.TIME = floor_date(DATE.TIME, unit = "1 minute"))%>%
        group_by(DATE.TIME) %>%
        summarise(
                MPVPosition = mean(MPVPosition, na.rm = TRUE),
                #N2O = mean(N2O, na.rm = TRUE),
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE)) %>%
        mutate(NH3 = NH3 / 1000)  # convert ppb to ppm

# write
write.csv(CRDS.P8, "CRDS.P8_inside.csv", row.names = FALSE)

#Picarro G2509
input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509"
output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
result_file_name <- "2024-08-27_2024-09-18_Picarro09"
CRDS.P9 <-  piconcatenate(input_path, output_path, result_file_name)
CRDS.P9 <- fread("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2024-08-27_2024-09-18_Picarro09.csv")

CRDS.P9 <- CRDS.P9 %>% mutate(DATE.TIME = as.POSIXct(paste(DATE, TIME), format = "%Y-%m-%d %H:%M:%S"))

CRDS.P9 <- CRDS.P9 %>%
        mutate(DATE.TIME = floor_date(DATE.TIME, unit = "2 minutes"))%>%
        group_by(DATE.TIME) %>%
        summarise(
                MPVPosition = mean(MPVPosition, na.rm = TRUE),
                N2O = mean(N2O, na.rm = TRUE),
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE)) %>%
        mutate(NH3 = NH3 / 1000)  # convert ppb to ppm

# write
write.csv(CRDS.P9, "CRDS.P9_outside.csv", row.names = FALSE)



################ hourly average for inside and oustide from CRDS in September ##################


######## Import Gas Data #########
CRDS.in <- fread("CRDS.P8_inside.csv")
CRDS.out <- fread("CRDS.P9_outside.csv")

CRDS.in <- CRDS.in  %>% filter(!is.na(MPVPosition) & MPVPosition %% 1 == 0 & MPVPosition != 0)
CRDS.out <- CRDS.out  %>% filter(!is.na(MPVPosition) & MPVPosition %% 1 == 0 & MPVPosition != 0)

CRDS.in <- CRDS.in %>%
        mutate(hour = floor_date(DATE.TIME, unit = "hour")) %>%
        group_by(hour) %>%
        summarise(
                CO2.in = mean(CO2, na.rm = TRUE),
                CH4.in = mean(CH4, na.rm = TRUE),
                NH3.in = mean(NH3, na.rm = TRUE),
                H2O.in = mean(H2O, na.rm = TRUE),
                .groups = "drop")

CRDS.out <- CRDS.out %>%
        mutate(hour = floor_date(DATE.TIME, unit = "hour")) %>%
        group_by(hour) %>%
        summarise(
                CO2.out = mean(CO2, na.rm = TRUE),
                CH4.out = mean(CH4, na.rm = TRUE),
                NH3.out = mean(NH3, na.rm = TRUE),
                H2O.out = mean(H2O, na.rm = TRUE),
                .groups = "drop")

######## Data combining ##########
# Format Date and time
CRDS.in$hour = as.POSIXct(CRDS.in$hour, format = "%Y-%m-%d %H:%M:%S")
CRDS.out$hour = as.POSIXct(CRDS.out$hour, format = "%Y-%m-%d %H:%M:%S")

# convert into data.table
data.table::setDT(CRDS.in)
data.table::setDT(CRDS.out)

# combine FTIR and CRDS
GAS.comb <- CRDS.in[CRDS.out, on = .(hour), roll = "nearest"]

# write
write.csv(GAS.comb, "2024_Aug_Sep_GAS.in_out.csv", row.names = FALSE)

