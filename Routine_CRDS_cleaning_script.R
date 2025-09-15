getwd()
####### libraries ######
library(tidyverse)
library(reshape2)
library(hablar)
library(lubridate)
library(psych)
library(ggplot2)
library(readxl)
library(dplyr)
library(ggpubr)
library(readr)
library(data.table)
source("Picarro_CRDS_data_cleaning_script.R")

####### 2025-05-08 to 2025-05-15 ATB Data importing and cleaning ########
CRDS9_20250508 <- piclean(
        input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509",
        gas = c("CO2", "CH4", "NH3", "H2O", "N2O"),
        start_time = "2025-05-08 12:00:00",
        end_time   = "2025-05-20 13:50:00",
        flush = 180, # in seconds
        interval = 450, # in seconds
)

# Create an hourly timestamp to group by
CRDS9_20250508$DATE.TIME <- as.POSIXct(CRDS9_20250508$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")
CRDS9_20250508$DATE.TIME <- floor_date(CRDS9_20250508$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
CRDS9_20250508_avg <- CRDS9_20250508 %>%
        select(-step_id) %>%
        group_by(DATE.TIME, MPVPosition) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                N2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )

CRDS9_20250508_avg <- CRDS9_20250508_avg %>%
        filter(MPVPosition %in% c(1, 2, 3)) %>%
        mutate(
                location = recode(as.factor(MPVPosition),
                                  `1` = "N",
                                  `2` = "in",
                                  `3` = "S"),
                lab = factor("ATB"),
                analyzer = factor("CRDS9")
        )

# Write long csv
CRDS9_20250508_avg <- CRDS9_20250508_avg %>% select(DATE.TIME, MPVPosition, location, lab, analyzer, everything())
CRDS9_20250508_avg$DATE.TIME <- format(CRDS9_20250508_avg$DATE.TIME, "%Y-%m-%d %H:%M:%S")
write.csv(CRDS9_20250508_avg,"ATB_CRDS9_20250508_hourly_gas_conc.csv" , row.names = FALSE, quote = FALSE)

# Change pivot to wide
CRDS9_20250508_inout <- CRDS9_20250508_avg %>%
        select(-MPVPosition) %>% 
        pivot_wider(
                names_from = location,
                values_from = c(CO2, CH4, NH3, H2O, N2O),
                names_sep = "_"
        )

# Write wide csv
write.csv(CRDS9_20250508_inout,"CRDS9_20250508_inout.csv" , row.names = FALSE, quote = FALSE)


####### 2025-05-22 to 2025-05-31 ATB Data importing and cleaning ########
CRDS9_20250522 <- piclean(
        input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509/2025",
        gas = c("CO2", "CH4", "NH3", "H2O", "N2O"),
        start_time = "2025-05-22 13:00:00",
        end_time   = "2025-06-23 11:55:00",
        flush = 180, # in seconds
        interval = 450) # in seconds

# Create an hourly timestamp to group by
CRDS9_20250522$DATE.TIME <- as.POSIXct(CRDS9_20250522$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")
CRDS9_20250522$DATE.TIME <- floor_date(CRDS9_20250522$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
CRDS9_20250522_avg <- CRDS9_20250522 %>%
        select(-step_id) %>%
        group_by(DATE.TIME, MPVPosition) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                N2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )

CRDS9_20250522_avg <- CRDS9_20250522_avg %>%
        filter(MPVPosition %in% c(1, 2, 3)) %>%
        mutate(
                location = recode(as.factor(MPVPosition),
                                  `1` = "N",
                                  `2` = "in",
                                  `3` = "S"),
                lab = factor("ATB"),
                analyzer = factor("CRDS9")
        )

# Write long csv
CRDS9_20250522_avg <- CRDS9_20250522_avg %>% select(DATE.TIME, MPVPosition, location, lab, analyzer, everything())
CRDS9_20250522_avg$DATE.TIME <- format(CRDS9_20250522_avg$DATE.TIME, "%Y-%m-%d %H:%M:%S")
write.csv(CRDS9_20250522_avg,"ATB_CRDS9_20250522_hourly_gas_conc.csv" , row.names = FALSE, quote = FALSE)

# Change pivot to wide
CRDS9_20250522_inout <- CRDS9_20250522_avg %>%
        select(-MPVPosition) %>% 
        pivot_wider(
                names_from = location,
                values_from = c(CO2, CH4, NH3, H2O, N2O),
                names_sep = "_"
        )

# Write wide csv
write.csv(CRDS9_20250522_inout,"CRDS9_20250522_inout.csv" , row.names = FALSE, quote = FALSE)


####### 2025-06-26 to 2025-07-10 ATB Data importing and cleaning ########
CRDS9_20250626 <- piclean(
        input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509/2025",
        gas = c("CO2", "CH4", "NH3", "H2O", "N2O"),
        start_time = "2025-06-26 11:00:00",
        end_time   = "2025-07-10 18:07:25",
        flush = 180, # in seconds
        interval = 450) # in seconds

# Create an hourly timestamp to group by
CRDS9_20250626$DATE.TIME <- as.POSIXct(CRDS9_20250626$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")
CRDS9_20250626$DATE.TIME <- floor_date(CRDS9_20250626$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
CRDS9_20250626_avg <- CRDS9_20250626 %>%
        select(-step_id) %>%
        group_by(DATE.TIME, MPVPosition) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                N2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )

CRDS9_20250626_avg <- CRDS9_20250626_avg %>%
        filter(MPVPosition %in% c(1, 2, 3)) %>%
        mutate(
                location = recode(as.factor(MPVPosition),
                                  `1` = "N",
                                  `2` = "in",
                                  `3` = "S"),
                lab = factor("ATB"),
                analyzer = factor("CRDS9")
        )

# Write long csv
CRDS9_20250626_avg <- CRDS9_20250626_avg %>% select(DATE.TIME, MPVPosition, location, lab, analyzer, everything())
CRDS9_20250626_avg$DATE.TIME <- format(CRDS9_20250626_avg$DATE.TIME, "%Y-%m-%d %H:%M:%S")
write.csv(CRDS9_20250626_avg,"ATB_CRDS9_20250626_hourly_gas_conc.csv" , row.names = FALSE, quote = FALSE)

# Change pivot to wide
CRDS9_20250626_inout <- CRDS9_20250626_avg %>%
        select(-MPVPosition) %>% 
        pivot_wider(
                names_from = location,
                values_from = c(CO2, CH4, NH3, H2O, N2O),
                names_sep = "_"
        )

# Write wide csv
write.csv(CRDS9_20250626_inout,"CRDS9_20250626_inout.csv" , row.names = FALSE, quote = FALSE)


####### 2025-07-10 to 2025-07-24 ATB Data importing and cleaning ########
CRDS9_20250710 <- piclean(
        input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509/2025",
        gas = c("CO2", "CH4", "NH3", "H2O", "N2O"),
        start_time = "2025-07-10 18:07:25",
        end_time   = "2025-07-24 18:07:25",
        flush = 180, # in seconds
        interval = 450) # in seconds

# Create an hourly timestamp to group by
CRDS9_20250710$DATE.TIME <- as.POSIXct(CRDS9_20250710$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")
CRDS9_20250710$DATE.TIME <- floor_date(CRDS9_20250710$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
CRDS9_20250710_avg <- CRDS9_20250710 %>%
        select(-step_id) %>%
        group_by(DATE.TIME, MPVPosition) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                N2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"  # To avoid warning about grouping
        )

CRDS9_20250710_avg <- CRDS9_20250710_avg %>%
        filter(MPVPosition %in% c(1, 2, 3)) %>%
        mutate(
                location = recode(as.factor(MPVPosition),
                                  `1` = "N",
                                  `2` = "in",
                                  `3` = "S"),
                lab = factor("ATB"),
                analyzer = factor("CRDS9")
        )

# Write long csv
CRDS9_20250710_avg <- CRDS9_20250710_avg %>% select(DATE.TIME, MPVPosition, location, lab, analyzer, everything())
CRDS9_20250710_avg$DATE.TIME <- format(CRDS9_20250710_avg$DATE.TIME, "%Y-%m-%d %H:%M:%S")
write.csv(CRDS9_20250710_avg,"ATB_CRDS9_20250710_hourly_gas_conc.csv" , row.names = FALSE, quote = FALSE)

# Change pivot to wide
CRDS9_20250710_inout <- CRDS9_20250710_avg %>%
        select(-MPVPosition) %>% 
        pivot_wider(
                names_from = location,
                values_from = c(CO2, CH4, NH3, H2O, N2O),
                names_sep = "_"
        )

# Write wide csv
write.csv(CRDS9_20250710_inout,"CRDS9_20250710_inout.csv" , row.names = FALSE, quote = FALSE)

####### 2025-07-24 to 2025-08-19 ATB Data importing and cleaning ########
CRDS9_20250724 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509/2025",
                       
                       gas = c("CO2", "CH4", "NH3", "H2O"),
                       
                       start_time = "2025-07-24 18:07:25",
                       
                       end_time = "2025-08-19 01:37:56",
                       
                       flush = 180, # Flush time in seconds
                       
                       interval = 450,  # Total time at MPVPosition in seconds
                       
                       MPVPosition.levels = c("1", "2", "3"),
                       
                       location.levels = c("N", "in", "S"),
                       
                       lab = "ATB",
                       
                       analyzer = "CRDS9")


# Create an hourly timestamp to group by
CRDS9_20250724$DATE.TIME <- as.POSIXct(CRDS9_20250724$DATE.TIME, format = "%Y-%m-%d %H:%M:%S")
CRDS9_20250724$DATE.TIME <- floor_date(CRDS9_20250724$DATE.TIME, "hour")

# Calculate the hourly average for each gas by MPVPosition
CRDS9_20250724_avg <- CRDS9_20250724 %>%
        select(-step_id) %>%
        group_by(DATE.TIME, MPVPosition, location, lab, analyzer) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                N2O = mean(H2O, na.rm = TRUE),
                .groups = "drop")  # To avoid warning about grouping


# Write long csv
CRDS9_20250724_avg$DATE.TIME <- format(CRDS9_20250724_avg$DATE.TIME, "%Y-%m-%d %H:%M:%S")
write.csv(CRDS9_20250724_avg,"ATB_CRDS9_20250724_hourly_gas_conc.csv" , row.names = FALSE, quote = FALSE)

# Change pivot to wide
CRDS9_20250724_inout <- CRDS9_20250724_avg %>%
        select(-MPVPosition) %>% 
        pivot_wider(names_from = location,
                    values_from = c(CO2, CH4, NH3, H2O, N2O),
                    names_sep = "_")
                

# Write wide csv
write.csv(CRDS9_20250724_inout,"CRDS9_20250724_inout.csv" , row.names = FALSE, quote = FALSE)


####### 2025-08-19 to 2025-08-28 ATB Data importing and cleaning ########
CRDS9_20250819 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509/2025",
                          
                          gas = c("CO2", "CH4", "NH3", "H2O"),
                          
                          start_time = "2025-08-19 13:59:00",
                          
                          end_time = "2025-08-28 01:30:59",
                          
                          flush = 60, # Flush time in seconds
                          
                          interval = 240,  # Total time at MPVPosition in seconds
                          
                          MPVPosition.levels = c("1", "2", "3", "4", "5", "6", "7", "8",
                                                 "9", "10", "11", "12", "13", "14", "15", "16"),
                          
                          location.levels = c("1", "3", "4", "6", "7", "9", "10", "12",
                                              "13", "15", "16", "18", "22", "24", "25", "27"),
                          
                          lab = "ATB",
                          
                          analyzer = "CRDS9")


CRDS8_20250819 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2025",
                          
                          gas = c("CO2", "CH4", "NH3", "H2O"),
                          
                          start_time = "2025-08-19 13:59:00",
                          
                          end_time = "2025-08-28 01:30:59",
                          
                          flush = 60, # Flush time in seconds
                          
                          interval = 240,  # Total time at MPVPosition in seconds
                          
                          MPVPosition.levels = c("1", "2", "3", "4", "5", "6", "7", "8", 
                                                 "9", "10", "11", "12", "13", "14", "15", "16"),
                          
                          location.levels = c("28", "30", "31", "33", "34", "36", "37", "39",
                                              "40", "42", "43", "45", "46", "48", "49", "51"),
                          
                          lab = "ATB",
                          
                          analyzer = "CRDS8")

CRDS_combined_df <- rbind(CRDS8_20250819, CRDS9_20250819) %>% arrange(DATE.TIME) %>%
        mutate(location = factor(location, levels = sort(unique(as.numeric(as.character(location))))))

#Data Visualization
ggline(CRDS_combined_df, 
       x = "location", 
       y = "CO2",
       add = "mean_se",         # Use mean ± SD
       error.plot = "errorbar", # Show error bars
       ylab = "Mean CO2 (ppm)",
       xlab = "MPV Position",
       title = "Mean CO2 ± SD by MPVPosition",
       color = "steelblue",
       add.params = list(width = 0.2)) +
        theme_minimal()

ggline(CRDS_combined_df, 
       x = "location", 
       y = "CH4",
       add = "mean_se",         # Use mean ± SD
       error.plot = "errorbar", # Show error bars
       ylab = "Mean CH4 (ppm)",
       xlab = "MPV Position",
       title = "Mean CH4 ± SD by MPVPosition",
       color = "green4",
       add.params = list(width = 0.2)) +
        theme_minimal()

ggline(CRDS_combined_df, 
       x = "location", 
       y = "NH3",
       add = "mean_se",         # Use mean ± SD
       error.plot = "errorbar", # Show error bars
       ylab = "Mean NH3 (ppm)",
       xlab = "MPV Position",
       title = "Mean NH3 ± SD by MPVPosition",
       color = "orange4",
       add.params = list(width = 0.2)) +
        theme_minimal()

# Calculate the hourly average of all MPVPosition
CRDS_combined_avg <- CRDS_combined_df %>%
        mutate(DATE.TIME = floor_date(CRDS_combined_df$DATE.TIME, "hour")) %>%
        select(-step_id, -MPVPosition, -measuring.time) %>%
        group_by(DATE.TIME,) %>%
        summarise(
                CO2_in = mean(CO2, na.rm = TRUE),
                CH4_in = mean(CH4, na.rm = TRUE),
                NH3_in = mean(NH3, na.rm = TRUE),
                H2O_in = mean(H2O, na.rm = TRUE),
                N2O_in = mean(H2O, na.rm = TRUE),
                .groups = "drop") 

write_excel_csv(CRDS_combined_avg, "H_CRDS_2025_08_19-2025_08_28.csv")

####### 2025-08-28 to 2025-09-05 ATB Data importing and cleaning ########
CRDS9_20250828 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509/2025",
                          
                          gas = c("CO2", "CH4", "NH3", "H2O"),
                          
                          start_time = "2025-08-28 11:59:00",
                          
                          end_time = "2025-09-05 11:34:41",
                          
                          flush = 60, # Flush time in seconds
                          
                          interval = 240,  # Total time at MPVPosition in seconds
                          
                          MPVPosition.levels = c("1", "2", "3", "4", "5", "6", "7", "8",
                                                 "9", "10", "11", "12", "13", "14", "15", "16"),
                          
                          location.levels = c("1", "3", "4", "6", "7", "9", "10", "12",
                                              "13", "15", "16", "18", "22", "24", "in", "out"),
                          
                          lab = "ATB",
                          
                          analyzer = "CRDS9")


CRDS8_20250828 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2025",
                          
                          gas = c("CO2", "CH4", "NH3", "H2O"),
                          
                          start_time = "2025-08-28 11:59:00",
                          
                          end_time = "2025-09-05 11:34:41",
                          
                          flush = 60, # Flush time in seconds
                          
                          interval = 240,  # Total time at MPVPosition in seconds
                          
                          MPVPosition.levels = c("1", "2", "3", "4", "5", "6", "7", "8", 
                                                 "9", "10", "11", "12", "13", "14", "15", "16"),
                          
                          location.levels = c("28", "30", "31", "33", "34", "36", "37", "39",
                                              "40", "42", "43", "45", "46", "48", "49", "51"),
                          
                          lab = "ATB",
                          
                          analyzer = "CRDS8")

CRDS_combined_df <- rbind(CRDS8_20250828, CRDS9_20250828) %>% arrange(DATE.TIME) %>%
        mutate(location = factor(location))

#Data Visualization
ggline(CRDS_combined_df, 
       x = "location", 
       y = "CO2",
       add = "mean_se",         # Use mean ± SD
       error.plot = "errorbar", # Show error bars
       ylab = "Mean CO2 (ppm)",
       xlab = "MPV Position",
       title = "Mean CO2 ± SD by MPVPosition",
       color = "steelblue",
       add.params = list(width = 0.2)) +
        theme_minimal()

ggline(CRDS_combined_df, 
       x = "location", 
       y = "CH4",
       add = "mean_se",         # Use mean ± SD
       error.plot = "errorbar", # Show error bars
       ylab = "Mean CH4 (ppm)",
       xlab = "MPV Position",
       title = "Mean CH4 ± SD by MPVPosition",
       color = "green4",
       add.params = list(width = 0.2)) +
        theme_minimal()

ggline(CRDS_combined_df, 
       x = "location", 
       y = "NH3",
       add = "mean_se",         # Use mean ± SD
       error.plot = "errorbar", # Show error bars
       ylab = "Mean NH3 (ppm)",
       xlab = "MPV Position",
       title = "Mean NH3 ± SD by MPVPosition",
       color = "orange4",
       add.params = list(width = 0.2)) +
        theme_minimal()

# Calculate the hourly average of all MPVPosition
CRDS_in_out <- CRDS_combined_df %>%
        filter(location %in% c("in","out")) %>%
        mutate(
                DATE.TIME = if_else(location == "out",
                                    DATE.TIME - seconds(450),  # shift out backwards
                                    DATE.TIME),
                DATE.HOUR = floor_date(DATE.TIME, "hour")
        ) %>%
        select(-step_id, -MPVPosition, -measuring.time) %>%
        group_by(DATE.HOUR, location) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"
        ) %>%
        tidyr::pivot_wider(
                names_from = location,
                values_from = c(CO2, CH4, NH3, H2O),
                names_sep = "_"
        )

write_excel_csv(CRDS_in_out, "H_CRDS_2025_08_28-2025-09-05.csv")


####### 2025-09-05 to 2025-09-12 ATB Data importing and cleaning ########
CRDS9_20250905 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509/2025",
                          
                          gas = c("CO2", "CH4", "NH3", "H2O"),
                          
                          start_time = "2025-09-05 11:34:41",
                          
                          end_time = "2025-09-12 10:03:57",
                          
                          flush = 60, # Flush time in seconds
                          
                          interval = 240,  # Total time at MPVPosition in seconds
                          
                          MPVPosition.levels = c("1", "2", "3", "4", "5", "6", "7", "8",
                                                 "9", "10", "11", "12", "13", "14", "15", "16"),
                          
                          location.levels = c("1", "3", "4", "6", "7", "9", "10", "12",
                                              "13", "15", "16", "18", "22", "24", "in", "out"),
                          
                          lab = "ATB",
                          
                          analyzer = "CRDS9")


CRDS8_20250905 <- piclean(input_path = "D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2508/2025",
                          
                          gas = c("CO2", "CH4", "NH3", "H2O"),
                          
                          start_time = "2025-09-05 11:31:05",
                          
                          end_time = "2025-09-12 10:07:00",
                          
                          flush = 60, # Flush time in seconds
                          
                          interval = 240,  # Total time at MPVPosition in seconds
                          
                          MPVPosition.levels = c("1", "2", "3", "4", "5", "6", "7", "8", 
                                                 "9", "10", "11", "12", "13", "14", "15", "16"),
                          
                          location.levels = c("28", "30", "31", "33", "34", "36", "37", "39",
                                              "40", "42", "43", "45", "46", "48", "49", "51"),
                          
                          lab = "ATB",
                          
                          analyzer = "CRDS8")

CRDS_combined_df <- rbind(CRDS8_20250905, CRDS9_20250905) %>% arrange(DATE.TIME) %>%
        mutate(location = factor(location))

#Data Visualization
ggline(CRDS_combined_df, 
       x = "location", 
       y = "CO2",
       add = "mean_se",         # Use mean ± SD
       error.plot = "errorbar", # Show error bars
       ylab = "Mean CO2 (ppm)",
       xlab = "MPV Position",
       title = "Mean CO2 ± SD by MPVPosition",
       color = "steelblue",
       add.params = list(width = 0.2)) +
        theme_minimal()

ggline(CRDS_combined_df, 
       x = "location", 
       y = "CH4",
       add = "mean_se",         # Use mean ± SD
       error.plot = "errorbar", # Show error bars
       ylab = "Mean CH4 (ppm)",
       xlab = "MPV Position",
       title = "Mean CH4 ± SD by MPVPosition",
       color = "green4",
       add.params = list(width = 0.2)) +
        theme_minimal()

ggline(CRDS_combined_df, 
       x = "location", 
       y = "NH3",
       add = "mean_se",         # Use mean ± SD
       error.plot = "errorbar", # Show error bars
       ylab = "Mean NH3 (ppm)",
       xlab = "MPV Position",
       title = "Mean NH3 ± SD by MPVPosition",
       color = "orange4",
       add.params = list(width = 0.2)) +
        theme_minimal()

# Calculate the hourly average of all MPVPosition
CRDS_in_out <- CRDS_combined_df %>%
        filter(location %in% c("in","out")) %>%
        mutate(
                DATE.TIME = if_else(location == "out",
                                    DATE.TIME - seconds(450),  # shift out backwards
                                    DATE.TIME),
                DATE.HOUR = floor_date(DATE.TIME, "hour")
        ) %>%
        select(-step_id, -MPVPosition, -measuring.time) %>%
        group_by(DATE.HOUR, location) %>%
        summarise(
                CO2 = mean(CO2, na.rm = TRUE),
                CH4 = mean(CH4, na.rm = TRUE),
                NH3 = mean(NH3, na.rm = TRUE),
                H2O = mean(H2O, na.rm = TRUE),
                .groups = "drop"
        ) %>%
        tidyr::pivot_wider(
                names_from = location,
                values_from = c(CO2, CH4, NH3, H2O),
                names_sep = "_"
        )

write_excel_csv(CRDS_in_out, "H_CRDS_20250905-20250912.csv")

