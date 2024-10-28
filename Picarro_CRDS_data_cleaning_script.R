# Libraries
library(dplyr)
library(lubridate)
library(progress)

####### Development of concatenate function ########
# Function to append and process data files
piconcatenate <- function(input_path, output_path, result_file_name, selected_columns) {
        # Task 1: Append data with selected columns only
        appendData <- function() {
                dat_files <- list.files(path = input_path, recursive = TRUE, pattern = "\\.dat$", full.names = TRUE)
                total_files <- length(dat_files)
                
                # Initialize a list to store data frames
                data_frames <- list()
                
                for (i in seq_along(dat_files)) {
                        current_data <- read.table(dat_files[i], header = TRUE)
                        
                        # Select only the specified columns
                        if (all(selected_columns %in% names(current_data))) {
                                current_data <- current_data[, selected_columns, drop = FALSE]
                        } else {
                                warning(sprintf("File %s does not contain all specified columns.", dat_files[i]))
                                next
                        }
                        
                        # Append data frame to the list
                        data_frames <- c(data_frames, list(current_data))
                        
                        # Print progress
                        cat(sprintf("Processed file %d of %d\n", i, total_files))
                }
                
                return(data_frames)
        }
        
        # Execute task
        list_of_data_frames <- appendData()
        
        # Combine the data frames into a single data frame
        merged_data <- do.call(rbind, list_of_data_frames)
        
        # Construct the full output path with .csv extension
        full_output_path <- file.path(output_path, paste0(result_file_name, ".csv"))
        
        # Write the merged data to a .csv file
        write.table(merged_data, full_output_path, sep = ",", row.names = FALSE)
        
        return(merged_data)
}

#### Example usage
#input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508"
#output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
#result_file_name <- "2024_June-Sep_Picarro08"
#selected_columns <- c("DATE.TIME", "MPVPosition", "OutletValve", "N2O", "CO2", "CH4", "H2O", "NH3")
#piconcatenate("path/to/input", "path/to/output", "result_file", "selected_columns")

