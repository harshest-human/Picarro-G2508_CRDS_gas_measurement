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
#input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508"
#output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
#result_file_name <- "2024-06-03_2024-06-11_Picarro08"
#start_date_time <- as.POSIXct("2024-06-03 15:13:00", format = "%Y-%m-%d %H:%M:%S")
#end_date_time <- as.POSIXct("2024-06-10 15:13:00", format = "%Y-%m-%d %H:%M:%S")
#CRDS.P8 <- piclean(input_path, output_path, result_file_name, start_date_time, end_date_time)
CRDS.P8 <- fread("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2024-06-03_2024-06-11_Picarro08.csv")
CRDS.P8 <- CRDS.P8 %>% rename_with(~paste0(., ".P8"), -DATE.TIME) # Add Suffix

#Picarro G2509
#input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509"
#output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
#result_file_name <- "2024-06-03_2024-06-11_Picarro09"
#start_date_time <- as.POSIXct("2024-06-03 15:13:00", format = "%Y-%m-%d %H:%M:%S")
#end_date_time <- as.POSIXct("2024-06-10 15:13:00", format = "%Y-%m-%d %H:%M:%S")
#CRDS.P9 <- piclean(input_path, output_path, result_file_name, start_date_time, end_date_time)
CRDS.P9 <- fread("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2024-06-03_2024-06-11_Picarro09.csv")
CRDS.P9 <- CRDS.P9 %>% rename_with(~paste0(., ".P9"), -DATE.TIME) # Add Suffix


####### Data combining ########
# Set data as data.table
data.table::setDT(CRDS.P8)
data.table::setDT(CRDS.P9)

CRDS.P8$DATE.TIME = as.POSIXct(CRDS.P8$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")
CRDS.P9$DATE.TIME = as.POSIXct(CRDS.P9$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")

# Combine two dataframes by nearest times using library(data.table)
CRDS.comb <- CRDS.P8[CRDS.P9, on = .(DATE.TIME), roll = "nearest"]

# Convert ppb to ppm 
CRDS.comb <- CRDS.comb %>%
        mutate(across(c("NH3.P8", "NH3.P9"), ~ . / 1000))

# write
write.csv(CRDS.comb, "CRDS.comb.csv", row.names = FALSE)
CRDS.comb <- fread("CRDS.comb.csv")


############ CRDS Test ###########
# function variables 
input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2024/05"
output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
result_file_name <- "2024-05-15_2024-05-17_CRDS.test"
start_date_time <- as.POSIXct("2024-05-15 12:39:56", format = "%Y-%m-%d %H:%M:%S")
end_date_time <- as.POSIXct("2024-05-17 09:39:05", format = "%Y-%m-%d %H:%M:%S")

# clean files
CRDS.test <- piconcatenate(input_path, output_path, result_file_name)

# read csv
CRDS.test <- fread("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2024-05-15_2024-05-17_CRDS.test.csv")

CRDS.test <- CRDS.test %>%
        mutate(DATE.TIME = as.POSIXct(paste(DATE, TIME), format = "%Y-%m-%d %H:%M:%S")) %>%
        select(DATE.TIME, MPVPosition, N2O, CO2, CH4, H2O, NH3)


CRDS.test <- CRDS.test %>%
        filter(DATE.TIME >= start_date_time & DATE.TIME <= end_date_time)

CRDS.test <- CRDS.test %>% filter(MPVPosition %% 1 == 0)

CRDS.test <- CRDS.test %>%
        mutate(DATE.TIME = floor_date(DATE.TIME, "4 minutes"))

CRDS.test <- CRDS.test %>%
        group_by(DATE.TIME) %>%
        summarize(MPVPosition = mean(MPVPosition, na.rm = TRUE),
                N2O = mean(N2O, na.rm = TRUE),
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE))

# Add Suffix
CRDS.test <- CRDS.test %>% rename_with(~paste0(., ".P8"), -DATE.TIME)

# write
write.csv(CRDS.test, "CRDS.test.csv", row.names = FALSE)
CRDS.test <- fread("CRDS.test.csv")





