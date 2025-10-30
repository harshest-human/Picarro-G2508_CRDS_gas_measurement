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
library(scales)
source("remove_outliers_function.R")

####### Import Picarro dataset #######
H_CRDS_20250828_20250925 <- read.csv("H_CRDS9_20250828_20250925.csv")
H_CRDS_20250925_20250930 <- read.csv("H_CRDS9_20250925_20250930.csv")
H_CRDS_20250930_20251010 <- read.csv("H_ATB_CRDS8+9_20250930_20251010.csv")
H_CRDS_20251010_20251024 <- read.csv("H_CRDS8+9_20251010_20251024.csv")

# Combine all into one data frame
CRDS_data <- bind_rows(
        H_CRDS_20250828_20250925,
        H_CRDS_20250925_20250930,
        H_CRDS_20250930_20251010,
        H_CRDS_20251010_20251024) %>%
        mutate(DATE.HOUR = ymd_hms(DATE.HOUR),        
               delta_CO2 = CO2_in - CO2_S,       
               delta_CH4 = CH4_in - CH4_S,
               delta_NH3 = NH3_in - NH3_S,
               delta_N2O = N2O_in - N2O_S) %>%
    select(DATE.HOUR, delta_CO2, delta_CH4, delta_NH3, delta_N2O,analyzer)


####### Import Pronova dataset #######
#  Set folder path
data_path <- "D:/Data Analysis/Gas_data/Pronova Data/Messdaten"

# List all logged data files
files <- list.files(
        path = data_path,
        pattern = "^differenzmessung_.*\\.txt$",
        full.names = TRUE
)

# Function to safely read one file
read_log_file <- function(file) {
        message("Reading: ", basename(file))
        df <- tryCatch(
                read.table(
                        file,
                        header = TRUE,
                        sep = "\t",
                        dec = ",",
                        check.names = FALSE,
                        stringsAsFactors = FALSE,
                        fill = TRUE,
                        comment.char = "",
                        colClasses = "character"   
                ),
                error = function(e) {
                        message("Error reading ", basename(file), ": ", e$message)
                        return(NULL)
                }
        )
        return(df)
}

# Read and combine all files safely
Pronova_data  <- files %>%
        lapply(read_log_file) %>%
        bind_rows() %>%
        mutate(DATETIME = dmy_hms(`Datum Uhrzeit`)) %>%
        filter(DATETIME > dmy_hms("28.08.2025 00:05:46")) %>%
        mutate(DATE.HOUR = floor_date(DATETIME, "hour")) %>%
        group_by(DATE.HOUR) %>%
        summarise(delta_CH4 = mean(as.numeric(gsub(",", ".", `CH4 in ppm`)), na.rm = TRUE),
                  delta_CO2 = mean(as.numeric(gsub(",", ".", `CO2 in ppm`)), na.rm = TRUE),
                  delta_N2O = mean(as.numeric(gsub(",", ".", `N2O in ppm`)), na.rm = TRUE),
                  delta_NH3 = mean(as.numeric(gsub(",", ".", `NH3 in ppm`)), na.rm = TRUE)) %>%
        mutate(analyzer = "Pronova")
                  
###### Combine Picarro and Pronova #######
Gas_data <- bind_rows(Pronova_data,CRDS_data) %>% arrange(DATE.HOUR)

Gas_oct <- Gas_data %>% filter(DATE.HOUR >= "2025-10-01 00:00:00",
                               DATE.HOUR <= "2025-10-24 00:00:00")

Gas_oct <- remove_outliers(Gas_oct, exclude_cols = c("DATE.HOUR", "analyzer"), group_cols = NULL)

###### Data visulation #######
delta_CO2_plot <- ggplot(Gas_oct, aes(x = DATE.HOUR, y = delta_CO2, color = analyzer)) +
        geom_line(size = 0.5) +           
        geom_point(size = 1, alpha = 0.7) +
        labs(title = "Time Series of Delta CO2 concentrations",
             x = "Date & Hour",
             y = expression(Delta~CO[2]~"(ppm)"),
             color = "Analyzer") +
        scale_x_datetime(date_breaks = "1 day",     
                         date_labels = "%d-%b") +
        theme_minimal() +
        theme(axis.text.x = element_text(angle = 45, hjust = 1),
              legend.position = "bottom")

delta_CH4_plot <- ggplot(Gas_oct, aes(x = DATE.HOUR, y = delta_CH4, color = analyzer)) +
        geom_line(size = 0.5) +           
        geom_point(size = 1, alpha = 0.7) +
        labs(title = "Time Series of Delta CH4 concentrations",
             x = "Date & Hour",
             y = expression(Delta~CH[4]~"(ppm)"),
             color = "Analyzer") +
        scale_x_datetime(date_breaks = "1 day",     
                         date_labels = "%d-%b") +
        theme_minimal() +
        theme(axis.text.x = element_text(angle = 45, hjust = 1),
              legend.position = "bottom")

delta_NH3_plot <- ggplot(Gas_oct, aes(x = DATE.HOUR, y = delta_NH3, color = analyzer)) +
        geom_line(size = 0.5) +           
        geom_point(size = 1, alpha = 0.7) +
        labs(title = "Time Series of Delta NH3 concentrations",
             x = "Date & Hour",
             y = expression(Delta~NH[3]~"(ppm)"),
             color = "Analyzer") +
        scale_x_datetime(date_breaks = "1 day",     
                         date_labels = "%d-%b") +
        theme_minimal() +
        theme(axis.text.x = element_text(angle = 45, hjust = 1),
              legend.position = "bottom")

delta_N2O_plot <- ggplot(Gas_oct, aes(x = DATE.HOUR, y = delta_N2O, color = analyzer)) +
        geom_line(size = 0.5) +           
        geom_point(size = 1, alpha = 0.7) +
        labs(title = "Time Series of Delta N2O concentrations",
             x = "Date & Hour",
             y = expression(Delta~N[2]*O~"(ppm)"),
             color = "Analyzer") +
        scale_x_datetime(date_breaks = "1 day",     
                         date_labels = "%d-%b") +
        theme_minimal() +
        theme(axis.text.x = element_text(angle = 45, hjust = 1),
              legend.position = "bottom")




# Define colors: darker for CRDS, lighter for Pronova
colors_CO2   <- c("CRDS9" = "#1f78b4", "Pronova" = "#a6cee3")
colors_CH4   <- c("CRDS9" = "#33a02c", "Pronova" = "#b2df8a")
colors_NH3   <- c("CRDS9" = "#ff7f00", "Pronova" = "#fdbf6f")
colors_N2O   <- c("CRDS9" = "#e31a1c", "Pronova" = "#fb9a99")

# Delta CO2
delta_CO2_plot <- Gas_oct %>%
        filter(!is.na(DATE.HOUR) & !is.na(delta_CO2)) %>%
        ggplot(aes(x = DATE.HOUR, y = delta_CO2, color = analyzer)) +
        geom_line(size = 0.7) +
        geom_point(size = 1.5, alpha = 0.7) +
        scale_color_manual(values = colors_CO2) +
        scale_x_datetime(date_breaks = "1 day", date_labels = "%d-%b") +
        labs(title = "Time Series of Delta CO2",
             x = "Date",
             y = expression(Delta~CO[2]~"(ppm)"),
             color = "Analyzer") +
        theme_minimal(base_size = 12) +
        theme(axis.text.x = element_text(angle = 45, hjust = 1),
              legend.position = "bottom")


# Delta CH4
delta_CH4_plot <- ggplot(Gas_oct, aes(x = DATE.HOUR, y = delta_CH4, color = analyzer)) +
        geom_line(size = 0.7) +
        geom_point(size = 1.5, alpha = 0.7) +
        scale_color_manual(values = colors_CH4) +
        scale_x_datetime(date_breaks = "1 day", date_labels = "%d-%b") +
        labs(title = "Time Series of Delta CH4",
             x = "Date",
             y = expression(Delta~CH[4]~"(ppm)"),
             color = "Analyzer") +
        theme_minimal(base_size = 12) +
        theme(axis.text.x = element_text(angle = 45, hjust = 1),
              legend.position = "bottom")

# Delta NH3
delta_NH3_plot <- ggplot(Gas_oct, aes(x = DATE.HOUR, y = delta_NH3, color = analyzer)) +
        geom_line(size = 0.7) +
        geom_point(size = 1.5, alpha = 0.7) +
        scale_color_manual(values = colors_NH3) +
        scale_x_datetime(date_breaks = "1 day", date_labels = "%d-%b") +
        labs(title = "Time Series of Delta NH3",
             x = "Date",
             y = expression(Delta~NH[3]~"(ppm)"),
             color = "Analyzer") +
        theme_minimal(base_size = 12) +
        theme(axis.text.x = element_text(angle = 45, hjust = 1),
              legend.position = "bottom")

# Delta N2O
delta_N2O_plot <- ggplot(Gas_oct, aes(x = DATE.HOUR, y = delta_N2O, color = analyzer)) +
        geom_line(size = 0.7) +
        geom_point(size = 1.5, alpha = 0.7) +
        scale_color_manual(values = colors_N2O) +
        scale_x_datetime(date_breaks = "1 day", date_labels = "%d-%b") +
        labs(title = "Time Series of Delta N2O",
             x = "Date",
             y = expression(Delta~N[2]*O~"(ppm)"),
             color = "Analyzer") +
        theme_minimal(base_size = 12) +
        theme(axis.text.x = element_text(angle = 45, hjust = 1),
              legend.position = "bottom")


# Set a folder to save plots (optional)
plot_dir <- "Picarro_vs_Pronova_Plots"
if(!dir.exists(plot_dir)) dir.create(plot_dir)

# Save each plot
ggsave(filename = file.path(plot_dir, "delta_CO2_plot.png"),
       plot = delta_CO2_plot,
       width = 10, height = 5, dpi = 300)

ggsave(filename = file.path(plot_dir, "delta_CH4_plot.png"),
       plot = delta_CH4_plot,
       width = 10, height = 5, dpi = 300)

ggsave(filename = file.path(plot_dir, "delta_NH3_plot.png"),
       plot = delta_NH3_plot,
       width = 10, height = 5, dpi = 300)

ggsave(filename = file.path(plot_dir, "delta_N2O_plot.png"),
       plot = delta_N2O_plot,
       width = 10, height = 5, dpi = 300)


###### Statitistical Analyses #######
# Compute mean relative error for each gas (without pivot_longer)
relative_error_summary <- Gas_oct %>%
        pivot_wider(
                names_from = analyzer,
                values_from = c(delta_CO2, delta_CH4, delta_N2O, delta_NH3)
        ) %>%
        summarise(
                RE_CO2 = mean(((delta_CO2_CRDS9 - delta_CO2_Pronova) / delta_CO2_Pronova) * 100, na.rm = TRUE),
                RE_CH4 = mean(((delta_CH4_CRDS9 - delta_CH4_Pronova) / delta_CH4_Pronova) * 100, na.rm = TRUE),
                RE_N2O = mean(((delta_N2O_CRDS9 - delta_N2O_Pronova) / delta_N2O_Pronova) * 100, na.rm = TRUE),
                RE_NH3 = mean(((delta_NH3_CRDS9 - delta_NH3_Pronova) / delta_NH3_Pronova) * 100, na.rm = TRUE)
        )

# Convert to data frame for plotting
relative_error_summary_df <- data.frame(
        Gas = c("CO2", "CH4", "N2O", "NH3"),
        Relative_Error = as.numeric(relative_error_summary[1, ])
) %>% filter(Gas %in% c("CO2", "CH4", "NH3"))

# --- Bar Plot ---
gas_colors <- c(
        "CO2" = "#1f78b4",
        "CH4" = "#33a02c",  
        "NH3" = "#ff7f00")

re_plot <- ggplot(relative_error_summary_df, aes(x = Gas, y = Relative_Error, fill = Gas)) +
        geom_col(width = 0.3) +
        geom_text(aes(label = sprintf("%.1f%%", Relative_Error)), vjust = -0.5, size = 4) +
        scale_fill_manual(values = gas_colors) +
        scale_y_continuous(breaks = seq(-100, 100, by = 5)) +
        labs(
                title = "Relative Error (%) Pronova - Picarro",
                x = NULL,
                y = "Relative Error (%)"
        ) +
        theme_minimal(base_size = 12) +
        theme(
                legend.position = "none",
                axis.text.x = element_text(face = "bold"),
                plot.title = element_text(hjust = 0.5)
        )

ggsave(filename = file.path(plot_dir, "Relative_error.png"),
       plot = re_plot,
       width = 6, height = 6, dpi = 300)
