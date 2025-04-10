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
gas <- c("CO2", "CH4", "NH3", "H2O")
start_time = "2025-04-08 12:00:00"
end_time = "2025-04-10 12:00:00"
flush = 180  # Flush time in seconds
interval = 450  # Interval for aggregation in seconds (can be changed to any value)

CRDS.P8 <- piclean(input_path, output_path, result_file_name, gas, start_time, end_time, flush, interval)



