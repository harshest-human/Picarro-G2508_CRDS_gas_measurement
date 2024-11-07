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
piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2024", output_path = "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean", result_file_name = "2024_June-Sep_CRDS.P8")  
CRDS.P8 <- fread("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2024_June-Sep_CRDS.P8.dat")
CRDS.P8 <- CRDS.P8 %>% rename_with(~paste0(., ".P8"), -DATE.TIME) # Add Suffix

#Picarro G2509 
piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509/2024", output_path = "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean", result_file_name = "2024_June-Sep_CRDS.P9")  
CRDS.P9 <- fread("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2024_June-Sep_CRDS.P9.dat")
CRDS.P9 <- CRDS.P9 %>% rename_with(~paste0(., ".P9"), -DATE.TIME) # Add Suffix


####### Data combining ########
# Set data as data.table
data.table::setDT(CRDS.P8)
data.table::setDT(CRDS.P9)


# Filter rows by 'DATE.TIME' range before merging
start_date <- as.POSIXct("2024-09-20 11:09:00")  # Set start date
end_date <- as.POSIXct("2024-10-21 12:29:00")  # Set end date


CRDS.P8$DATE.TIME = as.POSIXct(CRDS.P8$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")
CRDS.P9$DATE.TIME = as.POSIXct(CRDS.P9$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")

CRDS.P8 <- CRDS.P8[DATE.TIME >= start_date & DATE.TIME <= end_date]
CRDS.P9 <- CRDS.P9[DATE.TIME >= start_date & DATE.TIME <= end_date]

# Merge using nearest timestamp
CRDS.comb <- CRDS.P8[CRDS.P9, on = "DATE.TIME", roll = "nearest"] 


# write
write.csv(CRDS.comb, "2024_Sep_Oct_CRDS.comb.csv", row.names = FALSE) 


# Summary Statistics
# Count observations for each unique position in MPVPosition.P8
count_p8 <- CRDS.comb %>%
        group_by(MPVPosition.P8) %>%
        summarise(count = n())

# Count observations for each unique position in MPVPosition.P9
count_p9 <- CRDS.comb %>%
        group_by(MPVPosition.P9) %>%
        summarise(count = n())

