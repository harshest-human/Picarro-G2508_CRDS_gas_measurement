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
input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508"
output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
result_file_name <- "2024-08-27_2024-09-18_Picarro08"
CRDS.P8 <- piconcatenate(input_path, output_path, result_file_name)
CRDS.P8 <- fread("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2024-08-27_2024-09-18_Picarro08.csv")

CRDS.P8 <- CRDS.P8 %>% mutate(DATE.TIME = as.POSIXct(paste(DATE, TIME), format = "%Y-%m-%d %H:%M:%S"))

CRDS.P8 <- CRDS.P8 %>%
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





