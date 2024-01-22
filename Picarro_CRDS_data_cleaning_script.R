# Libraries
library(dplyr)
library(readr)
library(progress)
library(ggplot2)
library(ggpubr)

####### Development of function ########
# Function to merge data files
mergeDATFiles <- function(input_path, output_path, result_file_name) {
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
        
        # Write the combined data to a .dat file with well-spaced formatting
        write.table(merged_data, full_output_path, sep = " ", row.names = FALSE)
        
        # Read the data back
        CRDS_data <- read.table(full_output_path, header = TRUE)
        
        return(CRDS_data)
}


# Example usage
input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw"
output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
result_file_name <- "2023-12-19_2024-01-22_picarro_data"
CRDS_data <- mergeDATFiles(input_path, output_path, result_file_name)


####### Data Processing ########
CRDS_data <- read.table("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2023-12-19_2024-01-22_picarro_data.dat", sep="", header = TRUE)

# Remove non-integer values from MPVPosition and convert to factor
CRDS_data$MPVPosition <- as.factor(CRDS_data$MPVPosition[grepl("^\\d+$", CRDS_data$MPVPosition)])

# Convert N2O, CO2, CH4, H2O, NH3 to numeric
numeric_columns <- c("N2O", "CO2", "CH4", "H2O", "NH3")
CRDS_data[, numeric_columns] <- apply(CRDS_data[, numeric_columns], 2, as.numeric)

# Merge DATE and TIME to Date.time and format as yyyy-mm-dd HH:MM:SS
CRDS_data$Date.Time <- as.POSIXct(paste(CRDS_data$DATE, CRDS_data$TIME), format = "%Y-%m-%d %H:%M:%S")

# Select specific columns
CRDS_data <- select(CRDS_data, Date.Time, MPVPosition, N2O, CO2, CH4, H2O, NH3,-c(DATE, TIME))

# Plotting using ggline
ggline(CRDS_data, x="MPVPosition", y="CO2", add = "mean_se")

# Plotting using ggplot2 
ggplot(CRDS_data, aes(x=MPVPosition, y=CO2)) + geom_boxplot()

####### Data Analysis ########
