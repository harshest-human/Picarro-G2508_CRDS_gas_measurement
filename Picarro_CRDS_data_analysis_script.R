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
#Picarro G2508 (2025-04-08)
CRDS.P8 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2025/04",
                   
                   output_path = "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean",
                   
                   result_file_name = "20250408-09_Ring_7.5_cycle_CRDS",
                   
                   gas = c("CO2", "CH4", "NH3", "H2O"),
                   
                   start_time = "2025-04-08 12:00:00",
                   
                   end_time = "2025-04-09 23:59:59",
                   
                   flush = 180, # Flush time in seconds
                   
                   interval = 450) # Total time at MPVPosition in seconds


#read final result
CRDS.P8 <- fread("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/20250408-09_Ring_7.5_cycle_CRDS.csv")


