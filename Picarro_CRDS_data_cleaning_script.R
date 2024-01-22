# Libraries
library(dplyr)
library(readr)
library(progress)

# Function to merge data files
mergeDATFiles <- function(input_path, output_path, result_file_name) {
        # Task 1: Append data
        appendData <- function() {
                dat_files <- list.files(path = input_path, recursive = TRUE, pattern = "\\.dat$", full.names = TRUE)
                pb_append <- progress_bar$new(format = "[:bar] :percent :elapsed remaining: :eta", total = length(dat_files))
                
                merged_data <- data.frame()
                
                for (file in dat_files) {
                        current_data <- read.table(file, header = TRUE)
                        merged_data <- rbind(merged_data, current_data)
                        pb_append$tick()
                }
                
                pb_append$terminate()
                return(merged_data)
        }
        
        # Task 2: Clean & process data
        cleanProcessData <- function(merged_data) {
                # Cleaning data by removing unwanted variables
                merged_data <- select(merged_data, DATE, TIME, MPVPosition, N2O, CO2, CH4, H2O, NH3)
                return(merged_data)
        }
        
        # Execute tasks
        CRDS_data <- appendData()
        CRDS_data <- cleanProcessData(CRDS_data)
        
        # Construct the full output path
        full_output_path <- file.path(output_path, result_file_name)
        
        # Write the combined data to a CSV file
        write_csv(CRDS_data, full_output_path)
        
        # Read the final CSV file
        CRDS_data <- read_csv(full_output_path)
        
        return(CRDS_data)
}

# Example usage
input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw"
output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
result_file_name <- "2023-12-19_2024-01-22_picarro_data.csv"
CRDS_data <- mergeDATFiles(input_path, output_path, result_file_name)

