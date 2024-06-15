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


####### Data Analysis ########
#Remove outliers (1.5 IQR)
#remove_outliers_function <- source("remove_outliers_function.R")
#CRDS.P8_data$CO2 <- remove_outliers(CRDS.P8_data$CO2)
#CRDS.P8_data$CH4 <- remove_outliers(CRDS.P8_data$CH4)
#CRDS.P8_data$NH3 <- remove_outliers(CRDS.P8_data$NH3)

# Plotting using ggplot2 
ggplot(CRDS.comb, aes(x = factor(MPVPosition.P8), y = CO2.P8)) +
        geom_boxplot(aes(color = "MPVPosition.P8"), alpha = 0.5) +
        geom_boxplot(aes(x = factor(MPVPosition.P9), y = CO2.P9, color = "MPVPosition.P9"), alpha = 0.5) +
        labs(x = "MPVPosition", y = "CO2 Mean") +
        scale_color_manual(values = c("MPVPosition.P8" = "blue", "MPVPosition.P9" = "red"), 
                           labels = c("MPVPosition.P8", "MPVPosition.P9")) +
        theme_minimal()

# Plotting using ggline
ggline(CRDS.comb, x = "MPVPosition.P8", y = "CO2.P8",
       add = "mean_se",
       linetype = "solid",
       xlab = "MPVPosition", ylab = "CO2 Mean",
       legend = "right") 

# Plotting using ggline
ggline(CRDS.comb, x = "MPVPosition.P9", y = "CO2.P9",
       add = "mean_se",
       linetype = "solid",
       xlab = "MPVPosition", ylab = "CO2 Mean",
       legend = "right")



