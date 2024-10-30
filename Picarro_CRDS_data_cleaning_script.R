####### Development of picarro clean function ########
piclean <- function(input_path, output_path, result_file_name, columns) {
        library(dplyr)
        library(lubridate)
        
        # Function to append and process data files
        appendData <- function() {
                dat_files <- list.files(path = input_path, recursive = TRUE, pattern = "\\.dat$", full.names = TRUE)
                total_files <- length(dat_files)
                
                # Initialize a list to store data frames
                data_frames <- list()
                
                for (i in seq_along(dat_files)) {
                        current_data <- read.table(dat_files[i], header = TRUE)
                        
                        # Check for missing columns and select only the specified columns
                        existing_columns <- columns[columns %in% colnames(current_data)]
                        if (length(existing_columns) == 0) {
                                cat(sprintf("Warning: No matching columns found in file: %s\n", dat_files[i]))
                                next  # Skip this file
                        }
                        
                        current_data <- current_data[, existing_columns, drop = FALSE]
                        
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
        if (length(list_of_data_frames) == 0) {
                stop("No data frames were created. Please check your input files and column names.")
        }
        
        merged_data <- do.call(rbind, list_of_data_frames)
        
        # Merge DATE and TIME into DATE.TIME
        cat("Merging DATE and TIME column...\n")
        merged_data <- merged_data %>%
                mutate(DATE.TIME = as.POSIXct(paste(DATE, TIME), format = "%Y-%m-%d %H:%M:%S")) %>%
                select(-DATE, -TIME)  # Remove DATE and TIME columns
        
        # Check for non-integer MPVPosition values
        non_integer_mpv <- sum(merged_data$MPVPosition %% 1 != 0)
        total_mpv <- nrow(merged_data)
        
        cat("Removing non-integer MPVPosition values...\n")
        
        # Print the count and percentage of non-integer MPVPosition values
        if (non_integer_mpv > 0) {
                non_integer_percentage <- (non_integer_mpv / total_mpv) * 100
                cat("Non-integer MPVPosition values found =", non_integer_mpv, "/", total_mpv, "(", round(non_integer_percentage, 2), "%)", "\n")
                
                # Remove non-integer MPVPosition values
                merged_data <- merged_data %>% filter(MPVPosition %% 1 == 0)
                
                cat("Non-integer MPVPosition values removed. Remaining rows:", nrow(merged_data), "\n")
        } else {
                cat("No non-integer MPVPosition values found.\n")
        }
        
        # Check number of observations
        n_obs <- nrow(merged_data)
        
        # Construct the full output path with .dat extension
        full_output_path <- file.path(output_path, paste0(result_file_name, ".dat"))
        
        # Write final processed data to .dat file
        cat("Exporting the final dataframe...\n")
        write.table(merged_data, full_output_path, row.names = FALSE, sep = "\t", quote = FALSE)
        
        # Print final message
        cat("Data has been successfully processed and saved as .dat\n")
        cat("Dataframe contains", ncol(merged_data), "variables:\n", colnames(merged_data), "\n")
        
        # Return processed dataframe (optional, but generally not needed after writing to file)
        # return(merged_data)
}




#### Example usage
#piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2024", output_path = "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean", result_file_name = "test_CRDS.P8", columns = c("DATE", "TIME", "MPVPosition", "OutletValve", "N2O", "CO2", "CH4", "H2O", "NH3"))  

