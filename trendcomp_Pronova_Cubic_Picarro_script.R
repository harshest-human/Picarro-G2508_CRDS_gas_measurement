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
H_CRDS_20250828_20250925 <- read.csv("H_CRDS8+9_20250828_20250925.csv")
H_CRDS_20250925_20250930 <- read.csv("H_CRDS8+9_20250925_20250930.csv")
H_CRDS_20250930_20251010 <- read.csv("H_CRDS8+9_20250930_20251010.csv")
H_CRDS_20251010_20251024 <- read.csv("H_CRDS8+9_20251010_20251024.csv")
H_CRDS_20251024_20251030 <- read.csv("H_CRDS8+9_20251024_20251030.csv") 
H_CRDS_20251101_20251112 <- read.csv("H_CRDS8+9_20251101_20251112.csv") 
H_CRDS_20251112_20251114 <- read.csv("H_CRDS8+9_20251112_20251114.csv") 
H_CRDS_20251209_20251231 <- read.csv("H_CRDS8+9_20251209_20251231.csv") 
H_CRDS_20260101_20260119 <- read.csv("H_CRDS8+9_20260101_20260119.csv")
H_CRDS_20260217_20260223 <- read.csv("H_CRDS8+9_20260217_20260223.csv")

# Combine all into one data frame
CRDS_data <- bind_rows(
        H_CRDS_20250828_20250925,
        H_CRDS_20250925_20250930,
        H_CRDS_20250930_20251010,
        H_CRDS_20251010_20251024,
        H_CRDS_20251024_20251030,
        H_CRDS_20251101_20251112,
        H_CRDS_20251112_20251114,
        H_CRDS_20251209_20251231,
        H_CRDS_20260101_20260119,
        H_CRDS_20260217_20260223) %>%
        mutate(DATE.HOUR = ymd_hms(DATE.HOUR),        
               delta_CO2 = CO2_in - CO2_S,       
               delta_CH4 = CH4_in - CH4_S,
               delta_NH3 = NH3_in - NH3_S,
               delta_N2O = N2O_in - N2O_S) %>%
        select(DATE.HOUR, delta_CO2, delta_CH4, delta_NH3, delta_N2O,analyzer)

CRDS_data <- CRDS_data %>% 
        remove_outliers(exclude_cols = c("DATE.HOUR", "analyzer"),
                        group_cols = c("DATE.HOUR"))


####### Import Pronova dataset #######
#  Set folder path
pronova_path <- "D:/Data Analysis/Gas_data/Raw_data/Pronova Data/Messdaten"

# List all logged data files
pronova_files <- list.files(
        path = pronova_path,
        pattern = "^differenzmessung_.*\\.txt$",
        full.names = TRUE
)

# Function to safely read one file
pronova_read_log_file <- function(file) {
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
Pronova_data  <- pronova_files %>%
        lapply(pronova_read_log_file) %>%
        bind_rows() %>%
        mutate(DATE.TIME = dmy_hms(`Datum Uhrzeit`)) %>%
        mutate(DATE.HOUR = floor_date(DATE.TIME, "hour")) %>%
        group_by(DATE.HOUR) %>%
        summarise(delta_CH4 = mean(as.numeric(gsub(",", ".", `CH4 in ppm`)), na.rm = TRUE),
                  delta_CO2 = mean(as.numeric(gsub(",", ".", `CO2 in ppm`)), na.rm = TRUE),
                  delta_N2O = mean(as.numeric(gsub(",", ".", `N2O in ppm`)), na.rm = TRUE),
                  delta_NH3 = mean(as.numeric(gsub(",", ".", `NH3 in ppm`)), na.rm = TRUE)) %>%
        mutate(analyzer = "Pronova")


Pronova_data <- Pronova_data %>% 
        remove_outliers(exclude_cols = c("DATE.HOUR", "analyzer"),
                        group_cols = c("DATE.HOUR"))

####### Import Cubic dataset #########
# 1. Set the directory path
cubic_path <- "D:/Data Analysis/Gas_data/Raw_data/Cubic_raw"

# 2. List all .xlsx files
files <- list.files(path = cubic_path, 
                    pattern = "^[^~].*\\.xlsx$", 
                    full.names = TRUE)

# 3. Read all files and bind them row-wise
Cubic_data <- files %>%
        map_df(~ read_excel(.x)) %>%
        mutate(Time = ymd_hms(Time),
               DATE.HOUR = floor_date(Time, "hour")) %>%
        pivot_longer(cols = any_of(c("CH4", "NH3", "CO2")),
                     names_to = "gas", values_to = "value") %>%
        mutate(gas_name = paste0(gas, ifelse(Type == 1, "_in", "_S"))) %>%
        group_by(DATE.HOUR, gas_name) %>%
        summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%  # aggregate duplicates
        pivot_wider(names_from = gas_name, values_from = value) %>%
        arrange(DATE.HOUR) %>%
        mutate(DATE.HOUR = as.POSIXct(DATE.HOUR),        
               delta_CO2 = CO2_in - CO2_S,       
               delta_CH4 = CH4_in - CH4_S,
               delta_NH3 = NH3_in - NH3_S,
               analyzer = "Cubic") %>%
        select(DATE.HOUR, delta_CO2, delta_CH4, delta_NH3, analyzer)

Cubic_data <- Cubic_data %>% 
        remove_outliers(exclude_cols = c("DATE.HOUR", "analyzer"),
                        group_cols = c("DATE.HOUR"))

####### Combine Picarro, Pronova, and Cubic #######
Gas_data <- bind_rows(Pronova_data,CRDS_data, Cubic_data) %>% arrange(DATE.HOUR)
Gas_data <- remove_outliers(Gas_data, exclude_cols = c("DATE.HOUR", "analyzer"), group_cols = NULL)

####### Data visulation #######
trendcomp <- function(data, gas = "CO2", start_date, end_date, month_label, date_breaks = "1 day") {
        
        # 3-color scheme: CRDS red, Pronova blue, Cubic green
        gas_colors <- list(
                CO2 = c("CRDS9" = "black", "CRDS8" = "black", "Pronova" = "#1f78b4", "Cubic" = "#33a02c"),
                CH4 = c("CRDS9" = "black", "CRDS8" = "black", "Pronova" = "#1f78b4", "Cubic" = "#33a02c"),
                NH3 = c("CRDS9" = "black", "CRDS8" = "black", "Pronova" = "#1f78b4", "Cubic" = "#33a02c"),
                N2O = c("CRDS9" = "black", "CRDS8" = "black", "Pronova" = "#1f78b4", "Cubic" = "#33a02c")
        )
        
        # Select correct column dynamically
        gas_column <- paste0("delta_", gas)
        
        df <- data %>%
                filter(DATE.HOUR >= start_date,
                       DATE.HOUR <= end_date) %>%
                filter(!is.na(.data[[gas_column]]))
        
        y_labels <- list(
                CO2 = expression(Delta~CO[2]~"(ppm)"),
                CH4 = expression(Delta~CH[4]~"(ppm)"),
                NH3 = expression(Delta~NH[3]~"(ppm)"),
                N2O = expression(Delta~N[2]*O~"(ppm)")
        )
        
        # Line-only plot with transparency
        p <- ggplot(df, aes(DATE.HOUR, .data[[gas_column]], color = analyzer)) +
                geom_line(size = 0.9, alpha = 0.7) +   # transparent lines
                scale_color_manual(values = gas_colors[[gas]]) +
                scale_x_datetime(date_breaks = date_breaks, date_labels = "%d-%b") +
                labs(title = paste(month_label, "- Trend of Delta", gas),
                     x = "Date",
                     y = y_labels[[gas]],
                     color = "Analyzer") +
                theme_minimal(base_size = 12) +
                theme(axis.text.x = element_text(angle = 45, hjust = 1),
                      legend.position = "bottom",
                      plot.title = element_text(face = "bold"))
        
        return(p)
}


# October
trendcomp(Gas_data, "CO2",
          "2025-10-01 00:00:00",
          "2025-10-30 23:00:00",
          "October")

trendcomp(Gas_data, "CH4",
          "2025-10-01 00:00:00",
          "2025-10-30 23:00:00",
          "October")

trendcomp(Gas_data, "NH3",
          "2025-10-01 00:00:00",
          "2025-10-30 23:00:00",
          "October")

# November
trendcomp(Gas_data, "CO2",
          "2025-11-01 00:00:00",
          "2025-11-30 23:00:00",
          "November")

trendcomp(Gas_data, "CH4",
          "2025-11-01 00:00:00",
          "2025-11-30 23:00:00",
          "November")

trendcomp(Gas_data, "NH3",
          "2025-11-01 00:00:00",
          "2025-11-30 23:00:00",
          "November")

# December
trendcomp(Gas_data, "CO2",
          "2025-12-01 00:00:00",
          "2025-12-31 23:00:00",
          "December")

trendcomp(Gas_data, "CH4",
          "2025-12-01 00:00:00",
          "2025-12-31 23:00:00",
          "December")

trendcomp(Gas_data, "NH3",
          "2025-12-01 00:00:00",
          "2025-12-31 23:00:00",
          "December")

# January
trendcomp(Gas_data, "CO2",
          "2026-01-01 00:00:00",
          "2026-01-31 23:00:00",
          "January")

trendcomp(Gas_data, "CH4",
          "2026-01-01 00:00:00",
          "2026-01-31 23:00:00",
          "January")

trendcomp(Gas_data, "NH3",
          "2026-01-01 00:00:00",
          "2026-01-31 23:00:00",
          "January")

# February
trendcomp(Gas_data, "CO2",
          "2026-02-17 00:00:00",
          "2026-02-23 23:00:00",
          "February")

trendcomp(Gas_data, "CH4",
          "2026-02-17 00:00:00",
          "2026-02-23 23:00:00",
          "February")

trendcomp(Gas_data, "NH3",
          "2026-02-17 00:00:00",
          "2026-02-23 23:00:00",
          "February")


# Set a folder to save plots (optional)
plot_dir <- "Picarro_vs_Pronova_vs_Cubic_Plots"
if(!dir.exists(plot_dir)) dir.create(plot_dir)

months <- list(
        February  = c("2026-02-17 00:00:00",
                      "2026-02-23 23:00:00"))

gases <- c("CO2", "CH4", "NH3")

for (m in names(months)) {
        for (g in gases) {
                
                p <- trendcomp(Gas_data,
                               gas = g,
                               start_date = months[[m]][1],
                               end_date   = months[[m]][2],
                               month_label = m)
                
                filename <- paste0("Delta_", g, "_", m, ".png")
                
                ggsave(filename = file.path(plot_dir, filename),
                       plot = p,
                       width = 12,
                       height = 6,
                       dpi = 300)
        }
}

####### Data Analysis ######
Gas_dec <- Gas_data %>%
        select(-"delta_N2O") %>%
        filter(DATE.HOUR >= ymd_hms("2025-12-09 12:00:00"),
               DATE.HOUR <= ymd_hms("2026-01-01 01:00:00")) %>%
        pivot_wider(names_from = analyzer,
                    values_from = c(delta_CO2, delta_CH4, delta_NH3),
                    names_sep = "_")

Gas_dec_RE <- Gas_dec %>%
        mutate(RE_CO2_Pronova = (delta_CO2_Pronova - delta_CO2_CRDS8) / delta_CO2_CRDS8 * 100,
               RE_CO2_Cubic   = (delta_CO2_Cubic   - delta_CO2_CRDS8) / delta_CO2_CRDS8 * 100,
               
               RE_CH4_Pronova = (delta_CH4_Pronova - delta_CH4_CRDS8) / delta_CH4_CRDS8 * 100,
               RE_CH4_Cubic   = (delta_CH4_Cubic   - delta_CH4_CRDS8) / delta_CH4_CRDS8 * 100,
               
               RE_NH3_Pronova = (delta_NH3_Pronova - delta_NH3_CRDS8) / delta_NH3_CRDS8 * 100,
               RE_NH3_Cubic   = (delta_NH3_Cubic   - delta_NH3_CRDS8) / delta_NH3_CRDS8 * 100)

write.csv(Gas_dec_RE, "Gas_dec_RE_20251209-20251231.csv")

Gas_dec_RE <- read.csv("Gas_dec_RE_20251209-20251231.csv") %>%
        mutate(DATE.HOUR = parse_date_time(DATE.HOUR, orders = c("Y-m-d H:M:S", "Y-m-d")))

compute_drift <- function(df, gas, inst) {
        diff_col <- paste0("delta_", gas, "_", inst)
        ref_col  <- paste0("delta_", gas, "_CRDS8")
        
        df2 <- df %>%
                mutate(diff = .data[[diff_col]] - .data[[ref_col]],
                       t = as.numeric(DATE.HOUR - min(DATE.HOUR), units = "hours"))
        
        model <- lm(diff ~ t, data = df2)
        coef(model)[2]   # slope = drift (ppm per hour)
}

gases <- c("CO2", "CH4", "NH3")
instruments <- c("Pronova", "Cubic")

drift_results <- expand_grid(gas = gases, inst = instruments) %>%
        mutate(drift_ppm_per_hour = map2_dbl(gas, inst, ~compute_drift(Gas_dec_RE, .x, .y)))

compute_uncertainty <- function(df, gas, inst) {
        inst_col <- paste0("delta_", gas, "_", inst)
        ref_col  <- paste0("delta_", gas, "_CRDS8")
        
        diff <- df[[inst_col]] - df[[ref_col]]
        rmse <- sqrt(mean(diff^2, na.rm = TRUE))
        mean_ref <- mean(df[[ref_col]], na.rm = TRUE)
        
        tibble(
                gas = gas,
                instrument = inst,
                rmse = rmse,
                expanded_95 = 2 * rmse,
                rel_uncertainty_percent = rmse / mean_ref * 100
        )
}

uncertainty_results <- map_dfr(gases, function(g)
        map_dfr(instruments, function(i) compute_uncertainty(Gas_dec_RE, g, i))
)

final_stat_results <- drift_results %>%
        left_join(uncertainty_results,
                  by = c("gas", "inst" = "instrument"))


compute_correlation <- function(df, gas, inst) {
        inst_col <- paste0("delta_", gas, "_", inst)
        ref_col  <- paste0("delta_", gas, "_CRDS8")
        
        df %>%
                summarise(
                        gas = gas,
                        instrument = inst,
                        r = cor(.data[[inst_col]], .data[[ref_col]], use = "complete.obs"),
                        r2 = r^2
                )
}

gases <- c("CO2","CH4","NH3")
instruments <- c("Pronova","Cubic")

correlation_results <- map_dfr(gases, function(g)
        map_dfr(instruments, function(i) compute_correlation(Gas_dec_RE, g, i))
)


compute_ba <- function(df, gas, inst) {
        inst_col <- paste0("delta_", gas, "_", inst)
        ref_col  <- paste0("delta_", gas, "_CRDS8")
        
        diff <- df[[inst_col]] - df[[ref_col]]
        mean_vals <- (df[[inst_col]] + df[[ref_col]]) / 2
        
        bias <- mean(diff, na.rm = TRUE)
        sd_diff <- sd(diff, na.rm = TRUE)
        
        tibble(
                gas = gas,
                instrument = inst,
                bias = bias,
                loa_lower = bias - 1.96 * sd_diff,
                loa_upper = bias + 1.96 * sd_diff
        )
}

ba_results <- map_dfr(gases, function(g)
        map_dfr(instruments, function(i) compute_ba(Gas_dec_RE, g, i))
)

######## Statistical Visualization #########
out_dir <- "D:/Data Analysis/Picarro-G2508_CRDS_gas_measurement/Picarro_vs_Pronova_vs_Cubic_Plots"

if (!dir.exists(out_dir)) {
        dir.create(out_dir, recursive = TRUE)
}

plot_corr <- function(df, gas, inst) {
        inst_col <- paste0("delta_", gas, "_", inst)
        ref_col  <- paste0("delta_", gas, "_CRDS8")
        
        ggplot(df, aes(.data[[ref_col]], .data[[inst_col]])) +
                geom_point(alpha = 0.4) +
                geom_smooth(method = "lm", color = "blue") +
                labs(
                        title = paste("Correlation:", gas, inst),
                        x = "CRDS8 (ppm)",
                        y = paste(inst, "(ppm)")
                ) +
                theme_minimal()
}

for (g in gases) {
        for (i in instruments) {
                p <- plot_corr(Gas_dec_RE, g, i)
                ggsave(filename = file.path(out_dir, paste0("CORR_", g, "_", i, ".png")),
                       plot = p, width = 10, height = 6, dpi = 300)
        }
}
 
plot_ba <- function(df, gas, inst) {
        inst_col <- paste0("delta_", gas, "_", inst)
        ref_col  <- paste0("delta_", gas, "_CRDS8")
        
        df2 <- df %>%
                mutate(
                        mean_vals = (.data[[inst_col]] + .data[[ref_col]]) / 2,
                        diff = .data[[inst_col]] - .data[[ref_col]]
                )
        
        bias <- mean(df2$diff, na.rm = TRUE)
        sd_diff <- sd(df2$diff, na.rm = TRUE)
        
        ggplot(df2, aes(mean_vals, diff)) +
                geom_point(alpha = 0.4) +
                geom_hline(yintercept = bias, color = "blue", size = 1) +
                geom_hline(yintercept = bias + 1.96 * sd_diff, linetype = "dashed", color = "red") +
                geom_hline(yintercept = bias - 1.96 * sd_diff, linetype = "dashed", color = "red") +
                labs(
                        title = paste("Bland–Altman:", gas, inst),
                        x = "Mean concentration (ppm)",
                        y = "Difference (Instrument – CRDS8)"
                ) +
                theme_minimal()
}

instruments <- c("Pronova","Cubic")

for (g in gases) {
        for (i in instruments) {
                p <- plot_ba(Gas_dec_RE, g, i)
                ggsave(filename = file.path(out_dir, paste0("BA_", g, "_", i, ".png")),
                       plot = p, width = 10, height = 6, dpi = 300)
        }
}               
