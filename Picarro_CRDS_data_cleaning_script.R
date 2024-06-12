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
#input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508"
#output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
#result_file_name <- "2024-06-03_2024-06-11_Picarro08"
#CRDS_data <- mergeDATFiles(input_path, output_path, result_file_name)


####### Picarro G2508 Data Processing ########
input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508"
output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
result_file_name <- "2024-06-03_2024-06-11_Picarro08"
CRDS.P8_data <- mergeDATFiles(input_path, output_path, result_file_name)

# import the dataset
CRDS.P8_data <- read.csv("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2024-06-03_2024-06-11_Picarro08.dat", sep="")

# Find the number of non-integer values in MPVPosition
num_non_integers <- sum(!is.na(CRDS.P8_data$MPVPosition) & (CRDS.P8_data$MPVPosition %% 1 != 0))

# Find the number of zero values in MPVPosition
num_zeros <- sum(!is.na(CRDS.P8_data$MPVPosition) & CRDS.P8_data$MPVPosition == 0)

# Print the results
cat("Number of non-integer values in MPVPosition:", num_non_integers, "\n")
cat("Number of zero values in MPVPosition:", num_zeros, "\n")

# Remove non-integer values from MPVPosition and convert to factor
CRDS.P8_data <-  filter(CRDS.P8_data, MPVPosition %% 1 == 0 & MPVPosition != 0)

# Convert charachters into numeric and factors 
numeric_columns <- c("N2O", "CO2", "CH4", "H2O", "NH3")
CRDS.P8_data[, numeric_columns] <- apply(CRDS.P8_data[, numeric_columns], 2, as.numeric)
CRDS.P8_data$MPVPosition <- as.factor(CRDS.P8_data$MPVPosition)

# Merge DATE and TIME to Date.time and format as yyyy-mm-dd HH:MM:SS
CRDS.P8_data$Date.Time <- as.POSIXct(paste(CRDS.P8_data$DATE, CRDS.P8_data$TIME), format = "%Y-%m-%d %H:%M:%S")

# Select specific columns
CRDS.P8_data <- select(CRDS.P8_data, Date.Time, MPVPosition, N2O, CO2, CH4, H2O, NH3)

CRDS.P8_June <- CRDS.P8_data %>%
        filter(Date.Time > as.POSIXct("2024-06-03 15:13:00") & Date.Time < as.POSIXct("2024-06-11 10:45:00"))


####### Picarro G2509 Data Processing ########
input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509"
output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
result_file_name <- "2024-06-03_2024-06-11_Picarro09"
CRDS.P9_data <- mergeDATFiles(input_path, output_path, result_file_name)


####### Data Processing ########

# import the dataset
CRDS.P9_data <- read.csv("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2024-06-03_2024-06-11_Picarro09.dat", sep="")

# Find the number of non-integer values in MPVPosition
num_non_integers <- sum(!is.na(CRDS.P9_data$MPVPosition) & (CRDS.P9_data$MPVPosition %% 1 != 0))

# Find the number of zero values in MPVPosition
num_zeros <- sum(!is.na(CRDS.P9_data$MPVPosition) & CRDS.P9_data$MPVPosition == 0)

# Print the results
cat("Number of non-integer values in MPVPosition:", num_non_integers, "\n")
cat("Number of zero values in MPVPosition:", num_zeros, "\n")

# Remove non-integer values from MPVPosition and convert to factor
CRDS.P9_data <-  filter(CRDS.P9_data, MPVPosition %% 1 == 0 & MPVPosition != 0)

# Convert charachters into numeric and factors 
numeric_columns <- c("N2O", "CO2", "CH4", "H2O", "NH3")
CRDS.P9_data[, numeric_columns] <- apply(CRDS.P9_data[, numeric_columns], 2, as.numeric)
CRDS.P9_data$MPVPosition <- as.factor(CRDS.P9_data$MPVPosition)

# Merge DATE and TIME to Date.time and format as yyyy-mm-dd HH:MM:SS
CRDS.P9_data$Date.Time <- as.POSIXct(paste(CRDS.P9_data$DATE, CRDS.P9_data$TIME), format = "%Y-%m-%d %H:%M:%S")

# Select specific columns
CRDS.P9_data <- select(CRDS.P9_data, Date.Time, MPVPosition, N2O, CO2, CH4, H2O, NH3)

CRDS.P9_June <- CRDS.P9_data %>%
        filter(Date.Time > as.POSIXct("2024-06-03 15:13:00") & Date.Time < as.POSIXct("2024-06-11 10:49:00"))


# Rename columns of CRDS.P8_June
CRDS.P8_June <- CRDS.P8_June %>%
        rename_with(~paste0(., ".P8"), -Date.Time)

# Rename columns of CRDS.P9_June
CRDS.P9_June <- CRDS.P9_June %>%
        rename_with(~paste0(., ".P9"), -Date.Time)

# Merge the two data frames by Date.Time
CRDS_June.comb <- full_join(CRDS.P8_June, CRDS.P9_June, by = "Date.Time") %>%
        mutate(across(ends_with(".P8"), ~coalesce(., NA)),
               across(ends_with(".P9"), ~coalesce(., NA)))

# Convert Date.Time to POSIXct format
CRDS_June.comb$Date.Time <- as.POSIXct(CRDS_June.comb$Date.Time)

# Round Date.Time to the nearest 4-minute interval
CRDS_June.comb <- CRDS_June.comb %>%
        mutate(Group = round(as.numeric(Date.Time - min(Date.Time)) / (4 * 60))) %>%
        group_by(Group) %>%
        summarise(across(-Date.Time, ~ ifelse(all(is.na(.)), NA, mean(., na.rm = TRUE))))

# Fill NA values for the first minute
CRDS_June.comb[1, -1] <- colMeans(CRDS_June.comb[1:4, -1], na.rm = TRUE)


####### Data Analysis ########

#Remove outliers (1.5 IQR)
#remove_outliers_function <- source("remove_outliers_function.R")
#CRDS.P8_data$CO2 <- remove_outliers(CRDS.P8_data$CO2)
#CRDS.P8_data$CH4 <- remove_outliers(CRDS.P8_data$CH4)
#CRDS.P8_data$NH3 <- remove_outliers(CRDS.P8_data$NH3)

# Plotting using ggplot2 
ggplot(CRDS.P8_June, aes(x=MPVPosition, y=CO2)) + geom_boxplot()

# Plotting using ggline
ggline(CRDS.P8_June, x="MPVPosition", y="CO2", add = "mean_se")

# Plotting using ggplot2 
ggplot(CRDS.P9_June, aes(x=MPVPosition, y=CO2)) + geom_boxplot()

# Plotting using ggline
ggline(CRDS.P9_June, x="MPVPosition", y="CO2", add = "mean_se")

