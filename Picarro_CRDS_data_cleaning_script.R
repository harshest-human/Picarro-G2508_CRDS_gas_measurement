
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
piclean <- function(input_path, gas, start_time, end_time, flush, interval) {
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
                filter(!is.na(MPVPosition) & MPVPosition %% 1 == 0 & MPVPosition != 0) %>%
                arrange(DATE.TIME) %>%
                mutate(step_id = cumsum(c(1, diff(MPVPosition) != 0)))
        
        merged_data <- merged_data %>%
                group_by(step_id, MPVPosition) %>%
                arrange(DATE.TIME) %>%
                mutate(timestamp_for_step = last(DATE.TIME)) %>%
                mutate(time_rank = row_number()) %>%
                mutate(across(all_of(gas), ~ if_else(time_rank <= flush, NA_real_, .)))
        
        summarized <- merged_data %>%
                group_by(step_id, MPVPosition) %>%
                arrange(DATE.TIME) %>%
                mutate(time_rank = row_number()) %>%
                filter(time_rank <= interval) %>%
                summarise(
                        DATE.TIME = last(timestamp_for_step),
                        across(all_of(gas), ~ mean(.x, na.rm = TRUE)),
                        .groups = "drop"
                )
        
        # Remove step_id before final select
        summarized <- summarized %>%
                select(DATE.TIME, MPVPosition, everything())  # Reorder columns
        
        if ("NH3" %in% colnames(summarized)) {
                summarized <- summarized %>%
                        mutate(NH3 = NH3 / 1000)
        }
        
        cat("Number of representative observations for each MPVPosition level:\n")
        print(table(summarized$MPVPosition))
        
        # Format start and end times for file naming
        start_str <- format(as.POSIXct(start_time), "%Y%m%d%H%M%S")
        end_str <- format(as.POSIXct(end_time), "%Y%m%d%H%M%S")
        file_name <- paste0(start_str, "-", end_str, "_7.5min_gas_",".csv")
        full_output_path <- file.path(getwd(), file_name)
        
        cat("Exporting final data to file...\n")
        write.csv(summarized, full_output_path, row.names = FALSE, quote = FALSE)
        
        cat("✔ Data successfully processed and saved to:", full_output_path, "\n")
        cat("✔ Dataframe contains", ncol(summarized), "variables:\n", colnames(summarized), "\n")
        
        return(summarized)
}


#### Example usage
#piclean(
        #input_path = "data/",
        #gas = c("CO2", "CH4", "NH3"),
        #start_time = "2025-05-08 13:15:00",
        #end_time   = "2025-05-15 00:44:00",
        #flush = 60,
        #interval = 240)

