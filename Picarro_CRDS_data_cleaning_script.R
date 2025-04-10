
################# Development of piconcatenate function #######################

piconcatenate <- function(input_path, output_path, result_file_name, selected_columns) {
        
        library(dplyr)
        library(lubridate)
        
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



################# Development of piclean function #########################
################# Development of piclean function #########################
################# Development of piclean function #########################
piclean <- function(input_path, output_path, result_file_name, gas, start_time, end_time, flush, interval) {
        library(dplyr)
        library(lubridate)
        
        required_cols <- c("DATE", "TIME", "MPVPosition")
        all_needed_cols <- unique(c(required_cols, gas))
        
        appendData <- function() {
                dat_files <- list.files(path = input_path, recursive = TRUE, pattern = "\\.dat$", full.names = TRUE)
                total_files <- length(dat_files)
                data_frames <- list()
                
                for (i in seq_along(dat_files)) {
                        current_data <- read.table(dat_files[i], header = TRUE)
                        existing_cols <- all_needed_cols[all_needed_cols %in% colnames(current_data)]
                        if (length(existing_cols) == 0) {
                                cat(sprintf("Warning: No matching columns in file: %s\n", dat_files[i]))
                                next
                        }
                        current_data <- current_data[, existing_cols, drop = FALSE]
                        data_frames <- c(data_frames, list(current_data))
                        cat(sprintf("Processed file %d of %d\n", i, total_files))
                }
                
                return(data_frames)
        }
        
        list_of_data_frames <- appendData()
        
        if (length(list_of_data_frames) == 0) {
                stop("No data frames created. Check input files and column names.")
        }
        
        merged_data <- do.call(rbind, list_of_data_frames)
        
        cat("Merging DATE and TIME column...\n")
        merged_data <- merged_data %>%
                mutate(DATE.TIME = as.POSIXct(paste(DATE, TIME), format = "%Y-%m-%d %H:%M:%S")) %>%
                select(-DATE, -TIME)
        
        if (!is.null(start_time) && !is.null(end_time)) {
                cat("Filtering data between:\n",
                    "Start:", start_time, "\n",
                    "End:  ", end_time, "\n")
                merged_data <- merged_data %>%
                        filter(DATE.TIME >= as.POSIXct(start_time),
                               DATE.TIME <= as.POSIXct(end_time))
        }
        
        cat("Removing non-integer and zero MPVPosition values...\n")
        non_integer_mpv <- sum(!is.na(merged_data$MPVPosition) & 
                                       (merged_data$MPVPosition %% 1 != 0 | merged_data$MPVPosition == 0))
        total_mpv <- nrow(merged_data)
        cat("Removed", non_integer_mpv, "/", total_mpv, 
            "(", round(non_integer_mpv / total_mpv * 100, 2), "%)\n")
        
        merged_data <- merged_data %>%
                filter(!is.na(MPVPosition) & MPVPosition %% 1 == 0 & MPVPosition != 0)
        
        cat("Sorting and computing representative values per MPVPosition step...\n")
        merged_data <- merged_data %>% arrange(DATE.TIME)
        
        # Create an index based on MPVPosition changes
        merged_data <- merged_data %>%
                mutate(step_id = cumsum(c(1, diff(MPVPosition) != 0)))
        
        # Set the timestamp before applying flush time logic
        merged_data <- merged_data %>%
                group_by(step_id, MPVPosition) %>%
                arrange(DATE.TIME) %>%
                mutate(timestamp_for_step = last(DATE.TIME))  # Set the timestamp as the last observation in each step
        
        # Apply the flush time logic: replace gas values with NA during the flush time
        merged_data <- merged_data %>%
                group_by(step_id, MPVPosition) %>%
                arrange(DATE.TIME) %>%
                mutate(time_rank = row_number()) %>%
                # Use mutate to apply across gas columns and set them to NA during the flush time
                mutate(across(all_of(gas), ~ if_else(time_rank <= flush, NA_real_, .)))
        
        # Now aggregate the data over the given interval (interval - flush) and compute the mean
        summarized <- merged_data %>%
                group_by(step_id, MPVPosition) %>%
                arrange(DATE.TIME) %>%
                mutate(time_rank = row_number()) %>%
                filter(time_rank <= interval) %>%
                summarise(
                        DATE.TIME = last(timestamp_for_step),  # Carry the timestamp from the last observation
                        across(all_of(gas), ~ mean(.x, na.rm = TRUE)),
                        .groups = "drop"
                )
        
        # Remove the step_id column from the summarized data
        summarized <- summarized %>%
                select(-step_id) %>%
                select(DATE.TIME, MPVPosition, everything())  # Ensure correct column order
        
        if ("NH3" %in% colnames(summarized)) {
                summarized <- summarized %>%
                        mutate(NH3 = NH3 / 1000)
        }
        
        cat("Number of representative observations for each MPVPosition level:\n")
        print(table(summarized$MPVPosition))
        
        full_output_path <- file.path(output_path, paste0(result_file_name, ".dat"))
        cat("Exporting final data to file...\n")
        write.table(summarized, full_output_path, row.names = FALSE, sep = "\t", quote = FALSE)
        
        cat("✔ Data successfully processed and saved as .dat file\n")
        cat("✔ Dataframe contains", ncol(summarized), "variables:\n", colnames(summarized), "\n")
        
        return(summarized)
}




#### Example usage
#input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2025/04"
#output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
#result_file_name <- "20250408_Ring_concatenated"
#gas <- c("CO2", "CH4", "NH3", "H2O")
#start_time = "2025-04-08 12:00:00"
#end_time = "2025-04-08 22:07:30"
#flush = 180  # Flush time in seconds
#interval = 450  # Interval for aggregation in seconds (can be changed to any value)

#CRDS_data <- piclean(input_path, output_path, result_file_name, gas, start_time, end_time, flush, interval)

