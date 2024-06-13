# Libraries
library(dplyr)
library(readr)
library(progress)
library(ggplot2)
library(ggpubr)
library(lubridate)

####### Development of concatenate function ########
# Function to merge data files
picarro_concatenate <- function(input_path, output_path, result_file_name) {
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
        
}


# Example usage
#input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508"
#output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
#result_file_name <- "2024-06-03_2024-06-11_Picarro08"
#CRDS_data <- picarro_concatenate(input_path, output_path, result_file_name)

####### Development of bind function ########

picarro_bind <- function(input_path1, input_path2, output_path, result_file_name) {
        
        # Import data from first CSV
        data1 <- read.csv(input_path1, stringsAsFactors = FALSE)
        
        # Convert DATE and TIME to Date.Time and format as yyyy-mm-dd HH:MM:SS
        data1$Date.Time <- as.POSIXct(paste(data1$DATE, data1$TIME), format = "%Y-%m-%d %H:%M:%S")
        
        # Remove non-integer values from MPVPosition and convert to factor
        data1 <- data1 %>%
                filter(MPVPosition %% 1 == 0 & MPVPosition != 0) %>%
                mutate(MPVPosition = as.factor(MPVPosition))
        
        # Convert specified numeric columns to numeric
        numeric_columns <- c("N2O", "CO2", "CH4", "H2O", "NH3")
        data1[, numeric_columns] <- lapply(data1[, numeric_columns], as.numeric)
        
        # Select specific columns
        data1 <- select(data1, Date.Time, MPVPosition, N2O, CO2, CH4, H2O, NH3)
        
        # Import data from second CSV
        data2 <- read.csv(input_path2, stringsAsFactors = FALSE)
        
        # Convert DATE and TIME to Date.Time and format as yyyy-mm-dd HH:MM:SS
        data2$Date.Time <- as.POSIXct(paste(data2$DATE, data2$TIME), format = "%Y-%m-%d %H:%M:%S")
        
        # Remove non-integer values from MPVPosition and convert to factor
        data2 <- data2 %>%
                filter(MPVPosition %% 1 == 0 & MPVPosition != 0) %>%
                mutate(MPVPosition = as.factor(MPVPosition))
        
        # Convert specified numeric columns to numeric
        data2[, numeric_columns] <- lapply(data2[, numeric_columns], as.numeric)
        
        # Select specific columns
        data2 <- select(data2, Date.Time, MPVPosition, N2O, CO2, CH4, H2O, NH3)
        
        # Rename columns for merging
        data1_renamed <- data1 %>%
                rename_with(~paste0(., ".P1"), -Date.Time)
        
        data2_renamed <- data2 %>%
                rename_with(~paste0(., ".P2"), -Date.Time)
        
        # Merge datasets based on Date.Time
        combined_data <- full_join(data1_renamed, data2_renamed, by = "Date.Time") %>%
                mutate(across(ends_with(".P1"), ~coalesce(., NA)),
                       across(ends_with(".P2"), ~coalesce(., NA)))
        
        # Construct output file path
        output_file_path <- file.path(output_path, paste0(result_file_name, ".csv"))
        
        # Write the combined data to a CSV file
        write.csv(combined_data, file = output_file_path, row.names = FALSE)
        
        # Print confirmation message
        cat("Combined and cleaned data saved to:", output_file_path, "\n")
        
        # Return the combined dataframe (optional)
        return(combined_data)
}

# Example usage:
input_path1 <- read.csv("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2024-06-03_2024-06-11_Picarro08.dat", stringsAsFactors = FALSE, sep="")
input_path2 <- read.csv("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2024-06-03_2024-06-11_Picarro09.dat", stringsAsFactors = FALSE, sep="")
output_path <- "D:/Data Analysis/Gas_data/Combined_data"
result_file_name <- "2024-06-03_2024-06-11_combined_data"

CRDS.comb <- picarro_bind(input_path1, input_path2, output_path, result_file_name)

####### Picarro G2508 Data Processing ########
input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508"
output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
result_file_name <- "2024-06-03_2024-06-11_Picarro08"
CRDS.P8_data <- picarro_concatenate(input_path, output_path, result_file_name)

# import the dataset
CRDS.P8_data <- read.csv("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2024-06-03_2024-06-11_Picarro08.dat", sep="")

# Merge DATE and TIME to Date.time and format as yyyy-mm-dd HH:MM:SS
CRDS.P8_data$Date.Time <- as.POSIXct(paste(CRDS.P8_data$DATE, CRDS.P8_data$TIME), format = "%Y-%m-%d %H:%M:%S")

# Remove non-integer values from MPVPosition and convert to factor
CRDS.P8_data <-  filter(CRDS.P8_data, MPVPosition %% 1 == 0 & MPVPosition != 0)

# Convert charachters into numeric and factors 
numeric_columns <- c("N2O", "CO2", "CH4", "H2O", "NH3")
CRDS.P8_data[, numeric_columns] <- apply(CRDS.P8_data[, numeric_columns], 2, as.numeric)
CRDS.P8_data$MPVPosition <- as.factor(CRDS.P8_data$MPVPosition)

# Select specific columns
CRDS.P8_data <- select(CRDS.P8_data, Date.Time, MPVPosition, N2O, CO2, CH4, H2O, NH3)

CRDS.P8_June <- CRDS.P8_data %>%
        filter(Date.Time > as.POSIXct("2024-06-03 15:13:00") & Date.Time < as.POSIXct("2024-06-11 10:45:00"))


####### Picarro G2509 Data Processing ########
input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509"
output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
result_file_name <- "2024-06-03_2024-06-11_Picarro09"
CRDS.P9_data <- picarro_concatenate(input_path, output_path, result_file_name)


####### Import clean Data ########

# import the dataset
CRDS.P9_data <- read.csv("D:/Data Analysis/Gas_data/Clean_data/CRDS_clean/2024-06-03_2024-06-11_Picarro09.dat", sep="")

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

# Remove non-integer values
CRDS_June.comb <- CRDS_June.comb[!is.na(CRDS_June.comb$MPVPosition.P8) & !is.na(CRDS_June.comb$MPVPosition.P9), ]

# Write the combined data to a .dat file with well-spaced formatting
write.table(CRDS_June.comb, "CRDS_June.comb.csv", sep = ",", row.names = FALSE)


####### Combine clean Data in Time series########
CRDS_June.comb <- read.csv("D:/Data Analysis/Picarro-G2508_CRDS_gas_measurement/CRDS_June.comb.csv") %>% na.omit()

# Convert Date.Time to POSIXct format
CRDS_June.comb$Date.Time <- as.POSIXct(CRDS_June.comb$Date.Time)

# Round Date.Time to nearest minute
CRDS_June.comb$Date.Time <- round_date(CRDS_June.comb$Date.Time, "minute")

# Round Date.Time to the nearest minute
CRDS_June.comb <- CRDS_June.comb %>% aggregate(. ~ Date.Time, FUN = mean, na.rm = TRUE)




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

