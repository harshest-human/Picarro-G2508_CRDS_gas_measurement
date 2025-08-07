
################# Development of piclean function #########################
piclean <- function(input_path, gas, start_time, end_time, flush, interval, lab, analyzer) {
        library(dplyr)
        library(lubridate)
        library(readr)
        
        required_cols <- c("DATE", "TIME", "MPVPosition")
        all_needed_cols <- unique(c(required_cols, gas))
        
        # Step 1: Read and combine all .dat files
        appendData <- function() {
                dat_files <- list.files(path = input_path, recursive = TRUE, pattern = "\\.dat$", full.names = TRUE)
                total_files <- length(dat_files)
                data_frames <- list()
                
                for (i in seq_along(dat_files)) {
                        current_data <- tryCatch(
                                read.table(dat_files[i], header = TRUE, fill = TRUE, sep = "", stringsAsFactors = FALSE),
                                error = function(e) {
                                        cat(sprintf("Error reading file: %s\n", dat_files[i]))
                                        return(NULL)
                                }
                        )
                        if (is.null(current_data)) next
                        
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
        
        # Step 2: Time parsing and filtering
        cat("Merging DATE and TIME column...\n")
        merged_data <- merged_data %>%
                mutate(DATE.TIME = as.POSIXct(paste(DATE, TIME), format = "%Y-%m-%d %H:%M:%S", tz = "UTC")) %>%
                select(-DATE, -TIME)
        
        if (!is.null(start_time) && !is.null(end_time)) {
                cat("Filtering data between:\n",
                    "Start:", start_time, "\n",
                    "End:  ", end_time, "\n")
                merged_data <- merged_data %>%
                        filter(DATE.TIME >= as.POSIXct(start_time, tz = "UTC"),
                               DATE.TIME <= as.POSIXct(end_time, tz = "UTC"))
        }
        
        # Step 3: Clean MPVPosition and calculate step_id
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
        
        # Step 4: Interval binning
        fixed_interval_sec <- interval
        time_bins <- seq(from = as.POSIXct(start_time, tz = "UTC"), 
                         to = as.POSIXct(end_time, tz = "UTC"), 
                         by = fixed_interval_sec)
        merged_data <- merged_data %>%
                mutate(interval_bin = cut(DATE.TIME, breaks = time_bins, right = FALSE))
        
        # Step 5: Diagnostics
        interval_counts <- merged_data %>%
                group_by(interval_bin) %>%
                summarise(count = n(), .groups = "drop")
        
        abnormal_intervals <- interval_counts %>% filter(count == 0)
        cat("Step diagnostics:\n")
        cat("Total intervals:", length(time_bins)-1, "\n")
        cat("Intervals with zero measurements (missing data):", nrow(abnormal_intervals), "\n")
        if (nrow(abnormal_intervals) > 0) {
                print(abnormal_intervals)
        }
        
        # Step 6: Apply flush
        merged_data <- merged_data %>%
                group_by(interval_bin, MPVPosition) %>%
                arrange(DATE.TIME) %>%
                mutate(time_rank = row_number(),
                       timestamp_for_step = last(DATE.TIME)) %>%
                mutate(across(all_of(gas), ~ if_else(time_rank <= flush, NA_real_, .))) %>%
                ungroup()
        
        # Step 7: Summarize (avg after flush, limit to interval points)
        summarized <- merged_data %>%
                group_by(interval_bin, MPVPosition) %>%
                arrange(DATE.TIME) %>%
                mutate(time_rank = row_number()) %>%
                filter(time_rank <= interval) %>%
                summarise(
                        DATE.TIME = max(timestamp_for_step),
                        across(all_of(gas), ~ mean(.x, na.rm = TRUE)),
                        .groups = "drop"
                ) %>%
                filter(if_any(all_of(gas), ~ !is.na(.)))
        
        # Step 8: Add calculated mg/m3 values
        summarized <- summarized %>%
                mutate(
                        CO2_ppm = CO2,
                        CH4_ppm = CH4,
                        NH3_ppm = NH3,
                        H2O_vol = H2O,
                        R = 8.314472,
                        T = 273.15,
                        P = 1013.25 * 100,
                        CO2_mgm3 = (CO2_ppm / 1000 * 44.01 * P) / (R * T),
                        NH3_mgm3 = (NH3_ppm / 1000 * 17.031 * P) / (R * T),
                        CH4_mgm3 = (CH4_ppm / 1000 * 16.04 * P) / (R * T),
                        lab = lab,
                        analyzer = analyzer
                ) %>%
                select(DATE.TIME, MPVPosition, lab, analyzer, everything(), -R, -T, -P, -interval_bin, -all_of(gas)) %>%
                arrange(DATE.TIME, MPVPosition) %>%
                mutate(DATE.TIME = format(DATE.TIME, "%Y-%m-%d %H:%M:%S"))
        
        cat("Number of representative observations for each MPVPosition level:\n")
        print(table(summarized$MPVPosition))
        
        # Step 9: Write output
        start_str <- format(as.POSIXct(start_time, tz = "UTC"), "%Y%m%d%H%M%S")
        end_str <- format(as.POSIXct(end_time, tz = "UTC"), "%Y%m%d%H%M%S")
        file_name <- paste0(start_str, "-", end_str, "_", lab, "_", interval, "_avg_", analyzer, "_.csv")
        full_output_path <- file.path(getwd(), file_name)
        
        cat("Exporting final data to file...\n")
        readr::write_excel_csv(summarized, full_output_path)
        
        cat("✔ Data successfully processed and saved to:", full_output_path, "\n")
        cat("✔ Dataframe contains", ncol(summarized), "variables:\n", colnames(summarized), "\n")
        
        return(summarized)
}

#### Example usage
#piclean(
        #input_path = "./data_raw",
        #gas = c("CO2", "CH4", "NH3"),
        #start_time = "2025-04-08 12:00:00",
        #end_time = "2025-04-15 23:59:59",
        #flush = 180,
        #interval = 450,       # interval in seconds
        #lab = "ATB",
        #analyzer = "CRDS.1")


