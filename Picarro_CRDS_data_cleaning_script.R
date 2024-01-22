# Libraries
library(dplyr)
library(readr)
library(purrr)
library(progress)

# Function to merge data files
mergeDATFiles <- function(directory_path, output_file) {
        # Task 1: Append data
        appendData <- function() {
                dat_files <- list.files(path = directory_path, recursive = TRUE, pattern = "\\.dat$", full.names = TRUE)
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
                pb_clean <- progress_bar$new(format = "[:bar] :percent :elapsed remaining: :eta", total = 2)
                
                # Cleaning and processing of Data
                merged_data <- select(merged_data, DATE, TIME, MPVPosition, N2O, CO2, CH4, H2O, NH3) 
                
                # Group date and time column and format as date-time: yyyy-mm-dd hh:mm:ss
                merged_data$DateTime <- as.POSIXct(paste(merged_data$DATE, merged_data$TIME), format="%Y-%m-%d %H:%M:%S")
                merged_data <- select(merged_data, -c(DATE, TIME))  # Remove original date and time columns
                
                pb_clean$tick()
                
                merged_data <- merged_data %>% 
                        filter(MPVPosition == as.integer(MPVPosition)) %>%
                        na.omit("MPVPosition")       
                pb_clean$tick()
                
                pb_clean$terminate()
                return(merged_data)
        }
        
        # Task 3: Write as CSV
        writeAsCSV <- function(merged_data) {
                pb_write <- progress_bar$new(format = "[:bar] :percent :elapsed remaining: :eta", total = 1)
                
                # Write the combined data to a CSV file
                write_csv(merged_data, output_file)
                pb_write$tick()
                
                pb_write$terminate()
        }
        
        # Execute tasks
        merged_result <- appendData()
        merged_result <- cleanProcessData(merged_result)
        writeAsCSV(merged_result)
}

# Example usage
directory_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw"
output_file <- "2023-12-19_2024-01-22_picarro_data.csv"
merged_result <- mergeDATFiles(directory_path, output_file)

