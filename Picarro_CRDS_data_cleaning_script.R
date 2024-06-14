# Libraries
library(dplyr)
library(lubridate)

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

####### Development of clean function ########
piclean <- function(df) {
        
        # Select required columns
        df <- df[, c("DATE", "TIME", "MPVPosition", "N2O", "CO2", "CH4", "H2O", "NH3")]
        
        # Ask for start and end date-time
        start_date_time <- as.POSIXct(readline(prompt = "Enter the start date-time (YYYY-MM-DD HH:MM:SS): "), format = "%Y-%m-%d %H:%M:%S")
        end_date_time <- as.POSIXct(readline(prompt = "Enter the end date-time (YYYY-MM-DD HH:MM:SS): "), format = "%Y-%m-%d %H:%M:%S")
        
        # Merge DATE and TIME into DATE.TIME
        df$DATE.TIME <- as.POSIXct(paste(df$DATE, df$TIME), format = "%Y-%m-%d %H:%M:%S")
        
        # Filter data based on the provided date-time range
        df <- df %>%
                filter(DATE.TIME >= start_date_time & DATE.TIME <= end_date_time)
        
        # Count and print non-integer MPVPosition values
        non_integer_mpv <- sum(df$MPVPosition %% 1 != 0)
        total_mpv <- nrow(df)
        non_integer_percentage <- (non_integer_mpv / total_mpv) * 100
        cat("Non-integer MPVPosition values =", non_integer_mpv, "/", total_mpv, "(", round(non_integer_percentage, 2), "%)", "\n")
        
        # Ask whether to remove non-integer MPVPosition values
        remove_non_integer <- readline(prompt = "Do you want to omit the non-integer MPVPosition values? (y/n): ")
        if (tolower(remove_non_integer) == "y") {
                df <- df %>% filter(MPVPosition %% 1 == 0)
        }
        
        # Convert MPVPosition to factor and other columns to numeric
        df$MPVPosition <- as.factor(df$MPVPosition)
        numeric_columns <- c("N2O", "CO2", "CH4", "H2O", "NH3")
        df[numeric_columns] <- lapply(df[numeric_columns], as.numeric)
        
        # Aggregate data
        df_aggregated <- df %>%
                group_by(MPVPosition) %>%
                arrange(DATE.TIME) %>%
                mutate(Interval = floor(as.numeric(difftime(DATE.TIME, min(DATE.TIME), units = "secs")) / 240)) %>%
                filter(as.numeric(difftime(DATE.TIME, min(DATE.TIME), units = "secs")) %% 240 >= 60) %>%
                group_by(MPVPosition, Interval) %>%
                summarise(
                        DATE.TIME = max(DATE.TIME),
                        N2O = mean(N2O, na.rm = TRUE),
                        CO2 = mean(CO2, na.rm = TRUE),
                        CH4 = mean(CH4, na.rm = TRUE),
                        H2O = mean(H2O, na.rm = TRUE),
                        NH3 = mean(NH3, na.rm = TRUE)
                ) %>%
                ungroup() %>%
                select(DATE.TIME, MPVPosition, N2O, CO2, CH4, H2O, NH3) %>%
                arrange(DATE.TIME)
        
        return(df_aggregated)
}


# Example usage:
#CRDS_cleaned_data <- piclean(CRDS_data)

