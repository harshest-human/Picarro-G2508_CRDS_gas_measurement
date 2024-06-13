# Libraries
library(dplyr)
library(readr)
library(progress)
library(ggplot2)
library(ggpubr)
library(lubridate)

####### Development of concatenate function ########
# Function to append and process data files
picarro_concatenate <- function(input_path, output_path, result_file_name) {
        # Task 1: Append data without processing
        appendData <- function() {
                dat_files <- list.files(path = input_path, recursive = TRUE, pattern = "\\.dat$", full.names = TRUE)
                total_files <- length(dat_files)
                
                # Initialize a list to store data frames
                data_frames <- list()
                
                for (i in seq_along(dat_files)) {
                        current_data <- read.table(dat_files[i], header = TRUE)
                        
                        # Convert DATE and TIME to Date.Time and format as yyyy-mm-dd HH:MM:SS
                        if (all(c("DATE", "TIME") %in% colnames(current_data))) {
                                current_data$Date.Time <- as.POSIXct(paste(current_data$DATE, current_data$TIME), format = "%Y-%m-%d %H:%M:%S")
                        } else {
                                stop("Required columns (DATE, TIME) are missing from the data.")
                        }
                        
                        # Select specific columns
                        if (all(c("MPVPosition", "N2O", "CO2", "CH4", "H2O", "NH3") %in% colnames(current_data))) {
                                current_data <- select(current_data, Date.Time, MPVPosition, N2O, CO2, CH4, H2O, NH3)
                        } else {
                                stop("Required columns (MPVPosition, N2O, CO2, CH4, H2O, NH3) are missing from the data.")
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
        
        # Construct the full output path with .dat extension
        full_output_path <- file.path(output_path, paste0(result_file_name, ".dat"))
        
        # Write the combined data to a .dat file with well-spaced formatting
        write.table(merged_data, full_output_path, sep = " ", row.names = FALSE)
        
        # Return the combined data frame (optional)
        return(merged_data)
}

# Example usage
#input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508"
#output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
#result_file_name <- "2024-06-03_2024-06-11_Picarro08.P1"
#CRDS_data <- picarro_concatenate(input_path, output_path, result_file_name)


####### Data Processing ########
#Picarro G2508 
input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508"
output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
result_file_name <- "2024-06-03_2024-06-11_CRDS.P8"
CRDS.P8 <- picarro_concatenate(input_path, output_path, result_file_name)

#Picarro G2509
input_path <- "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509"
output_path <- "D:/Data Analysis/Gas_data/Clean_data/CRDS_clean"
result_file_name <- "2024-06-03_2024-06-11_CRDS.P9"
CRDS.P9 <- picarro_concatenate(input_path, output_path, result_file_name)


# Import the dataset and filter by measurement campaign
CRDS.P8_data <- CRDS.P8 %>%
        filter(Date.Time > as.POSIXct("2024-06-03 15:13:00") & Date.Time < as.POSIXct("2024-06-11 10:49:00")) %>%
        rename_with(~paste0(., ".P8"), -Date.Time)

CRDS.P9_data <- CRDS.P9 %>%
        filter(Date.Time > as.POSIXct("2024-06-03 15:13:00") & Date.Time < as.POSIXct("2024-06-11 10:49:00")) %>%
        rename_with(~paste0(., ".P9"), -Date.Time)


# Merge the two data frames by Date.Time
CRDS.comb <- full_join(CRDS.P8_data, CRDS.P9_data, by = "Date.Time") %>%
        mutate(across(ends_with(".P8"), ~coalesce(., NA)),
               across(ends_with(".P9"), ~coalesce(., NA)))

CRDS.comb <- CRDS.comb %>%
        
        # Remove non-integer values from MPVPosition columns
        filter(MPVPosition.P8 %% 1 == 0 & MPVPosition.P8 != 0 & 
                       MPVPosition.P9 %% 1 == 0 & MPVPosition.P9 != 0) %>%
        
        # Convert MPVPosition columns to factors
        mutate(MPVPosition.P8 = as.factor(MPVPosition.P8),
               MPVPosition.P9 = as.factor(MPVPosition.P9)) %>%
        # Omit rows with NA values
        na.omit()

# Write the combined data to a .dat file with well-spaced formatting
write.table(CRDS.comb, "CRDS.comb.csv", sep = ",", row.names = FALSE)


####### Clean Data in Time series########
#remove repeated observation
CRDS.comb_data <- CRDS.comb %>%
        distinct(Date.Time, .keep_all = TRUE)

# Round Date.Time to the nearest minute
CRDS.comb_data <- CRDS.comb %>%
        mutate(Date.Time = round(Date.Time, "mins"))

CRDS.comb_data$Date.Time <- as.POSIXct(CRDS.comb_data$Date.Time, format = "%Y-%m-%d %H:%M:%S")

# Aggregate by Date.Time and MPVPosition
CRDS.comb_agr <- CRDS.comb_data %>%
        group_by(Date.Time, MPVPosition.P8, MPVPosition.P9) %>%
        summarise(
                N2O.P8 = mean(N2O.P8, na.rm = TRUE),
                CO2.P8 = mean(CO2.P8, na.rm = TRUE),
                CH4.P8 = mean(CH4.P8, na.rm = TRUE),
                H2O.P8 = mean(H2O.P8, na.rm = TRUE),
                NH3.P8 = mean(NH3.P8, na.rm = TRUE),
                N2O.P9 = mean(N2O.P9, na.rm = TRUE),
                CO2.P9 = mean(CO2.P9, na.rm = TRUE),
                CH4.P9 = mean(CH4.P9, na.rm = TRUE),
                H2O.P9 = mean(H2O.P9, na.rm = TRUE),
                NH3.P9 = mean(NH3.P9, na.rm = TRUE))

####### Data Analysis ########

#Remove outliers (1.5 IQR)
#remove_outliers_function <- source("remove_outliers_function.R")
#CRDS.P8_data$CO2 <- remove_outliers(CRDS.P8_data$CO2)
#CRDS.P8_data$CH4 <- remove_outliers(CRDS.P8_data$CH4)
#CRDS.P8_data$NH3 <- remove_outliers(CRDS.P8_data$NH3)

# Plotting using ggplot2 
ggplot(CRDS.comb_agr, aes(x = factor(MPVPosition.P8), y = CO2.P8)) +
        geom_boxplot(aes(color = "MPVPosition.P8"), alpha = 0.5) +
        geom_boxplot(aes(x = factor(MPVPosition.P9), y = CO2.P9, color = "MPVPosition.P9"), alpha = 0.5) +
        labs(x = "MPVPosition", y = "CO2 Mean") +
        scale_color_manual(values = c("MPVPosition.P8" = "blue", "MPVPosition.P9" = "red"), 
                           labels = c("MPVPosition.P8", "MPVPosition.P9")) +
        theme_minimal()

# Plotting using ggline
ggline(CRDS.comb_agr, x = "MPVPosition.P8", y = "CO2.P8",
       add = "mean_se",
       linetype = "solid",
       xlab = "MPVPosition", ylab = "CO2 Mean",
       legend = "right") 

# Plotting using ggline
ggline(CRDS.comb_agr, x = "MPVPosition.P9", y = "CO2.P9",
       add = "mean_se",
       linetype = "solid",
       xlab = "MPVPosition", ylab = "CO2 Mean",
       legend = "right") 


