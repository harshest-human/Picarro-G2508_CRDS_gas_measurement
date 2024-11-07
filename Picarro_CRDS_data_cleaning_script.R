####### Development of picarro clean function ########
piclean <- function(input_path, output_path, result_file_name) {
        library(dplyr)
        library(lubridate)
        
        # Define the required columns to select before concatenating
        columns <- c("DATE", "TIME", "MPVPosition", "OutletValve", "N2O", "CO2", "CH4", "H2O", "NH3")
        
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
        
        # Merge DATE and TIME into DATE.TIME and remove DATE and TIME columns
        cat("Merging DATE and TIME column...\n")
        merged_data <- merged_data %>%
                mutate(DATE.TIME = as.POSIXct(paste(DATE, TIME), format = "%Y-%m-%d %H:%M:%S")) %>%
                select(-DATE, -TIME)
        
        # Check for and remove non-integer MPVPosition values
        cat("Removing non-integer MPVPosition values...\n")
        
        # Identify non-integer values
        non_integer_mpv <- sum(!is.na(merged_data$MPVPosition) & merged_data$MPVPosition %% 1 != 0)
        total_mpv <- nrow(merged_data)
        non_integer_percentage <- (non_integer_mpv / total_mpv) * 100
        
        # Log the count and percentage of non-integer values
        cat("Non-integer MPVPosition values removed =", non_integer_mpv, "/", total_mpv, "(", round(non_integer_percentage, 2), "%)", "\n")
        
        # Filter out rows with non-integer MPVPosition values
        merged_data <- merged_data %>% filter(!is.na(MPVPosition) & MPVPosition %% 1 == 0 & MPVPosition != 0)
        
        # Temporally average each MPVPosition over 4-minute intervals, taking last 180 seconds if available
        cat("Averaging data at each MPVPosition interval...\n")
        processed_data <- merged_data %>%
                arrange(DATE.TIME) %>%
                group_by(MPVPosition) %>%
                mutate(
                        Interval = cumsum(c(0, diff(DATE.TIME) > 240))  # Track intervals based on gaps longer than 4 minutes
                ) %>%
                group_by(MPVPosition, Interval) %>%
                filter(n() >= 180) %>%  # Allow intervals with at least 180 observations
                slice_tail(n = 180) %>%  # Take the last 180 seconds for averaging
                summarise(
                        DATE.TIME = last(DATE.TIME),  # Use the last timestamp in the interval
                        N2O = mean(N2O, na.rm = TRUE),
                        CO2 = mean(CO2, na.rm = TRUE),
                        CH4 = mean(CH4, na.rm = TRUE),
                        H2O = mean(H2O, na.rm = TRUE),
                        NH3 = mean(NH3, na.rm = TRUE) / 1000,  # Divide NH3 by 1000
                        OutletValve = last(OutletValve) # Take the last status of OutletValve
                ) %>%
                select(DATE.TIME, everything()) %>%
                select(-Interval) %>%
                arrange(DATE.TIME)  # Sort by DATE.TIME in final output
        
        # Construct the full output path with .dat extension
        full_output_path <- file.path(output_path, paste0(result_file_name, ".dat"))
        
        # Write final processed data to .dat file
        cat("Exporting the final dataframe...\n")
        write.table(processed_data, full_output_path, row.names = FALSE, sep = "\t", quote = FALSE)
        
        # Print final message
        cat("Data has been successfully processed and saved as .dat\n")
        cat("Dataframe contains", ncol(processed_data), "variables:\n", colnames(processed_data), "\n")
        
        # Optionally, return the processed dataframe
        return(processed_data)
}


#### Example usage
#piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2024", output_path = "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean", result_file_name = "test_CRDS.P8")  

