# Libraries
library(dplyr)
library(lubridate)
library(progress)

####### Development of concatenate function ########
# Function to append and process data files
piconcatenate <- function(input_path, output_path, result_file_name) {
        # Task 1: Append data without processing
        appendData <- function() {
                dat_files <- list.files(path = input_path, recursive = TRUE, pattern = "\\.dat$", full.names = TRUE)
                total_files <- length(dat_files)
                
                # Initialize a list to store data frames
                data_frames <- list()
                
                for (i in seq_along(dat_files)) {
                        current_data <- read.table(dat_files[i], header = TRUE)
                        
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
        
        # Construct the full output path with .dat extension
        full_output_path <- file.path(output_path, paste0(result_file_name, ".dat"))
        
        # Write the merged data to a .dat file
        write.table(merged_data, full_output_path, sep = " ", row.names = FALSE)
        
        return(merged_data)
}


# Example usage
#input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508"
#output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
#result_file_name <- "2024-06-03_2024-06-11_Picarro08"
#CRDS_data <- piconcatenate(input_path, output_path, result_file_name)

####### Development of picarro clean function ########
piclean <- function(input_path, output_path, result_file_name, start_date_time, end_date_time) {
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
        
        # Select required columns
        cat("Selecting required columns...\n")
        merged_data <- merged_data[, c("DATE", "TIME", "MPVPosition", "N2O", "CO2", "CH4", "H2O", "NH3")]
        
        # Merge DATE and TIME into DATE.TIME
        cat("Merging DATE and TIME column...\n")
        merged_data <- merged_data %>%
                mutate(DATE.TIME = as.POSIXct(paste(DATE, TIME), format = "%Y-%m-%d %H:%M:%S")) %>%
                select(DATE.TIME, MPVPosition, N2O, CO2, CH4, H2O, NH3)
        
        # Filter data based on the provided date-time range
        cat("Filtering data...\n")
        merged_data <- merged_data %>%
                filter(DATE.TIME >= start_date_time & DATE.TIME <= end_date_time)
        
        # Print non-integer MPVPosition values
        non_integer_mpv <- sum(merged_data$MPVPosition %% 1 != 0)
        total_mpv <- nrow(merged_data)
        non_integer_percentage <- (non_integer_mpv / total_mpv) * 100
        
        cat("Non-integer MPVPosition values removed =", non_integer_mpv, "/", total_mpv, "(", round(non_integer_percentage, 2), "%)", "\n")
        
        # Remove non-integer MPVPosition values
        merged_data <- merged_data %>% filter(MPVPosition %% 1 == 0)
        
        # Check number of observations
        n_obs <- nrow(merged_data)
        time_diff_seconds <- as.numeric(difftime(end_date_time, start_date_time, units = "secs"))
        
        # If number of observations doesn't match seconds between start and end time, take averages
        if (n_obs != time_diff_seconds + 1) {
                cat("Warning: Number of observations (", n_obs, ") does not match total seconds between start and end time (", time_diff_seconds + 1, ")\n")
                cat("Time series processing: Rounding-off to observation per second\n")
                
                # Aggregate data to handle duplicate seconds
                merged_data <- merged_data %>%
                        group_by(DATE.TIME) %>%
                        summarise_all(mean) %>%
                        ungroup() %>%
                        arrange(DATE.TIME) %>%
                        select(-starts_with("X"))
        }
        
        # Further aggregate data by grouping MPVPosition
        cat("Further aggregating data by MPVPosition...\n")
        merged_data <- merged_data %>%
                arrange(DATE.TIME) %>%
                group_by(MPVPosition) %>%
                mutate(
                        Interval = cumsum(c(0, diff(DATE.TIME) > 60)),  # Identify intervals where time difference is greater than 60 seconds
                        RowNum = row_number()  # Number of rows within each group
                ) %>%
                filter(RowNum > 60) %>%  # Skip the first 60 observations
                group_by(MPVPosition, Interval) %>%
                summarise(
                        DATE.TIME = last(DATE.TIME),  # Use the last timestamp before switching MPVPosition
                        N2O = mean(N2O, na.rm = TRUE),
                        CO2 = mean(CO2, na.rm = TRUE),
                        CH4 = mean(CH4, na.rm = TRUE),
                        H2O = mean(H2O, na.rm = TRUE),
                        NH3 = mean(NH3, na.rm = TRUE)
                ) %>%
                ungroup() %>%
                select(DATE.TIME, MPVPosition, N2O, CO2, CH4, H2O, NH3) %>%
                arrange(DATE.TIME)
        
        # Print final message
        cat("Data has been successfully processed\n")
        cat("Dataframe contains", ncol(merged_data), "variables:\n", colnames(merged_data), "\n")
        
        # Write final processed data to CSV
        write.csv(merged_data, full_output_path, row.names = FALSE)
        
        # Return processed dataframe (optional, but generally not needed after writing to file)
        # return(merged_data)
}


# Example usage

#input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508"
#output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
#result_file_name <- "2024-06-03_2024-06-11_Picarro08"
#start_date_time <- as.POSIXct("2024-06-03 15:13:00", format = "%Y-%m-%d %H:%M:%S")
#end_date_time <- as.POSIXct("2024-06-10 15:13:00", format = "%Y-%m-%d %H:%M:%S")
#CRDS.P8 <- piclean(input_path, output_path, result_file_name, start_date_time, end_date_time)
#CRDS.P8 <- read.csv("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2024-06-03_2024-06-11_Picarro08.csv")
