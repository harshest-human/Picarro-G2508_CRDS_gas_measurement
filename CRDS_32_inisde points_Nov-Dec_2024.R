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
piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2024", output_path = "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean", result_file_name = "2024_Dec_01_to_31_CRDS.P8")  
CRDS.P8 <- fread("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2024_Dec_01_to_31_CRDS.P8.dat")
CRDS.P8 <- CRDS.P8 %>% rename_with(~paste0(., ".P8"), -DATE.TIME) # Add Suffix

#Picarro G2509 
piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509/2024", output_path = "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean", result_file_name = "2024_Dec_01_to_31_CRDS.P9")  
CRDS.P9 <- fread("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2024_Dec_01_to_31_CRDS.P9.dat")
CRDS.P9 <- CRDS.P9 %>% rename_with(~paste0(., ".P9"), -DATE.TIME) # Add Suffix


####### Data combining ########
# Convert CRDS.P8 and CRDS.P9 to data.table format
data.table::setDT(CRDS.P8)
data.table::setDT(CRDS.P9)

# Convert DATE.TIME columns to POSIXct format
CRDS.P8[, DATE.TIME := as.POSIXct(DATE.TIME, format = "%Y-%m-%d %H:%M:%S")]
CRDS.P9[, DATE.TIME := as.POSIXct(DATE.TIME, format = "%Y-%m-%d %H:%M:%S")]

# Define the start and end dates for the time sequence
start_date <- as.POSIXct("2024-12-01 00:00:00")
end_date <- as.POSIXct("2024-12-31 00:00:00")

CRDS.P9 <- CRDS.P9[DATE.TIME >= start_date & DATE.TIME <= end_date]
CRDS.P8 <- CRDS.P8[DATE.TIME >= start_date & DATE.TIME <= end_date]

# Set up a sequence of 4-minute intervals from min to max DATE.TIME
time_seq1 <- data.table(DATE.TIME = seq(min(CRDS.P9$DATE.TIME), max(CRDS.P9$DATE.TIME), by = "4 min"))
time_seq2 <- data.table(DATE.TIME = seq(min(CRDS.P8$DATE.TIME), max(CRDS.P8$DATE.TIME), by = "4 min"))

# Merge data with the full time sequence to fill gaps
complete_data <- merge(full_time_seq, data, by = "DATE.TIME", all.x = TRUE)

# Merge using nearest timestamp
CRDS.comb <- CRDS.P8[CRDS.P9, on = "DATE.TIME", roll = "nearest"] 


# write
write.csv(CRDS.comb, "2024_Dec_01_to_31_CRDS.comb.csv", row.names = FALSE) 


# Summary Statistics
# Count observations for each unique position in MPVPosition.P8
count_p8 <- CRDS.comb %>%
        group_by(MPVPosition.P8) %>%
        summarise(count = n())

# Count observations for each unique position in MPVPosition.P9
count_p9 <- CRDS.comb %>%
        group_by(MPVPosition.P9) %>%
        summarise(count = n())


# Create a new dataframe with the desired structure
CRDS.long <- data.table(
        DATE.TIME = CRDS.comb$DATE.TIME,
        ID = rep(c("MPVPosition.P8", "MPVPosition.P9"), each = nrow(CRDS.comb)),
        sampling.point = c(CRDS.comb$MPVPosition.P9, CRDS.comb$MPVPosition.P8),
        CO2 = c(CRDS.comb$CO2.P9, CRDS.comb$CO2.P8),
        CH4 = c(CRDS.comb$CH4.P9, CRDS.comb$CH4.P8),
        NH3 = c(CRDS.comb$NH3.P9, CRDS.comb$NH3.P8),
        H2O = c(CRDS.comb$H2O.P9, CRDS.comb$H2O.P8))

# Convert 'GAS.long' to data.table
setDT(CRDS.long)

# write after arranging columns
CRDS.long <- CRDS.long[, .(DATE.TIME, ID, sampling.point, CO2, CH4, NH3, H2O)]
setorder(CRDS.long, DATE.TIME)

write.csv(CRDS.long, "2024_Dec_01_to_31_CRDS.long.csv", row.names = FALSE)

CRDS.long <- fread("2024_Dec_01_to_31_CRDS.long.csv")

CRDS.long_in <- CRDS.long %>%
        mutate(hour = floor_date(DATE.TIME, unit = "hour")) %>%
        group_by(hour) %>%
        summarise(
                CO2.in = mean(CO2, na.rm = TRUE),
                CH4.in = mean(CH4, na.rm = TRUE),
                NH3.in = mean(NH3, na.rm = TRUE),
                H2O.in = mean(H2O, na.rm = TRUE),
                .groups = "drop")

write.csv(CRDS.long_in, "2024_Dec_01_to_31_CRDS.inside.csv", row.names = FALSE)
