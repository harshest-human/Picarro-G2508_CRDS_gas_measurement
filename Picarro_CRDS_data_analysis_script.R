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

####### Data concatenating ########
#Picarro G2508 
input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508"
output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
result_file_name <- "2024-06-03_2024-06-11_CRDS.P8"
CRDS.P8 <- piconcatenate(input_path, output_path, result_file_name)

#Picarro G2509
input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509"
output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
result_file_name <- "2024-06-03_2024-06-11_CRDS.P9"
CRDS.P9 <- piconcatenate(input_path, output_path, result_file_name)


####### Data cleaning ########
#Picarro G2508
cleaned.P8_data <- piclean(CRDS.P8) 
cleaned.P8_data <- cleaned.P8_data %>%
rename_with(~paste0(., ".P8"), -DATE.TIME)

#Picarro G2509
cleaned.P9_data <- piclean(CRDS.P9) 
cleaned.P9_data <- cleaned.P9_data %>%
        rename_with(~paste0(., ".P9"), -DATE.TIME)


####### Data combining ########
# Set data as data.table
data.table::setDT(cleaned.P8_data)
data.table::setDT(cleaned.P9_data)

# Combine two dataframes by nearest times using library(data.table)
CRDS.comb <- cleaned.P8_data[cleaned.P9_data, on = .(DATE.TIME), roll = "nearest"]


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


############ HEAT MAP #############

# Melt data to long format for heatmap
melted_data <-melt(CRDS.comb, id.vars = c("MPVPosition.P8", "MPVPosition.P9"),
                   measure.vars = c("CO2.P8", "CO2.P9"),
                   variable.name = "GasType", value.name = "MeanValue")

# Plot heatmap
ggplot(melted_data, aes(x = MPVPosition.P8, y = MPVPosition.P9, fill = MeanValue)) +
        geom_tile(color = "white") +
        scale_fill_gradient(low = "white", high = "blue") +
        labs(x = "MPVPosition.P8", y = "MPVPosition.P9", fill = "Mean CO2") +
        theme_minimal()


