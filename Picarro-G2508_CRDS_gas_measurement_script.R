# Set the working directory to the folder containing the subfolders
setwd("D:/Data Analysis/Picarro-G2508_CRDS_gas_measurement")
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
library(zip)
library(data.table)
library(purrr)

# Set the working directory to the folder containing the subfolders
setwd("D:/Data Analysis/Picarro-G2508_CRDS_gas_measurement/picrarro_G2509_june_2023")

# Get a list of subfolders
subfolders <- c("01", "02", "03", "04", "05")

# Create an empty data frame to store the combined data
combined_data <- data.frame()

# Loop through each subfolder
for (subfolder in subfolders) {
        # Set the working directory to the current subfolder
        setwd(subfolder)
        
        # Get a list of .dat files in the current subfolder
        dat_files <- list.files(pattern = "\\.dat$")
        
        # Loop through each .dat file
        for (dat_file in dat_files) {
                # Read the data from the current .dat file
                data <- read.table(dat_file, header = TRUE)  # Modify the parameters if needed
                
                # Append the data to the combined_data data frame
                combined_data <- rbind(combined_data, data)
        }
        
        # Return to the parent folder
        setwd("..")
}

# Write the combined data to a CSV file
write.csv(combined_data, "picarro_combined_june_2023_data.csv", row.names = FALSE)





combined_data <- read_csv("D:/Data Analysis/Picarro-G2508_CRDS_gas_measurement/combined_data.csv")

picarro_input <- read_csv("combined_data.csv") 


picarro_input <- select(picarro_input, DATE, TIME,MPVPosition, N2O, CO2, CH4, H2O, NH3) 
picarro_input <- picarro_input %>% 
        filter(MPVPosition == as.integer(MPVPosition)) %>%
        na.omit("MPVPosition")       
picarro_input$MPVPosition <- as.factor(picarro_input$MPVPosition)
picarro_input$DATE <- as.Date(picarro_input$DATE)

Remove_outliers_function <- source("D:/HARSHY DATA 2020/Master Thesis/Ansyco FTIR Data/Ansyco_FTIR_modelling/remove_outliers_function.R")
picarro_input$CO2 <- remove_outliers(picarro_input$CO2)
picarro_input$CH4 <- remove_outliers(picarro_input$CH4)
picarro_input$NH3 <- remove_outliers(picarro_input$NH3)

#1 MPVxCO2 New Line (After  milking parlour)
MPVxCO2 <- select(picarro_input,MPVPosition, CO2)
plot(CO2~MPVPosition, data=MPVxCO2, main = "MPVxCO2")

#2 MPVxCH4_dry New Line (After  milking parlour)
MPVxCH4 <- select(picarro_input,MPVPosition, CH4) 
plot(CH4~MPVPosition, data=MPVxCH4, main = "MPVxCH4")


#3 MPVxNH3 New Line (After  milking parlour)
MPVxNH3 <- select(picarro_input,MPVPosition, NH3) 
plot(NH3~MPVPosition, data=MPVxNH3, main = "MPVxNH3")



