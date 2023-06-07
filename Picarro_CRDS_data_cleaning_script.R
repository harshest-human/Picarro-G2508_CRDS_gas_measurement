library(dplyr)
library(readr)
library(purrr)

# Create an empty data frame to store the combined data
combined_data <- data.frame()

# Define the subfolders
subfolders <- c("01", "02", "03", "04", "05")

# Loop through each subfolder
for (subfolder in subfolders) {
        # Set the path to the current subfolder
        subfolder_path <- file.path("picrarro_G2509_june_2023", subfolder)
        
        # Get a list of .dat files in the current subfolder
        dat_files <- list.files(path = subfolder_path, pattern = "\\.dat$", full.names = TRUE)
        
        # Loop through each .dat file
        for (dat_file in dat_files) {
                # Read the data from the current .dat file
                data <- read.table(dat_file, header = TRUE)  # Modify the parameters if needed
                
                # Append the data to the combined_data data frame
                combined_data <- rbind(combined_data, data)
        }
}


# Cleanning and processing of Data
combined_data <- select(combined_data, DATE, TIME,MPVPosition, N2O, CO2, CH4, H2O, NH3) 
combined_data <- combined_data %>% 
        filter(MPVPosition == as.integer(MPVPosition)) %>%
        na.omit("MPVPosition")       
combined_data$MPVPosition <- as.factor(combined_data$MPVPosition)
combined_data$DATE <- as.Date(combined_data$DATE)

# Write the combined data to a CSV file
write.csv(combined_data, "picarro_combined_june_2023_data.csv", row.names = FALSE)

