
################# Development of piclean function #########################
piclean <- function(input_path, gas, start_time, end_time, flush, interval, lab, analyzer) {
        library(dplyr)
        library(lubridate)
        library(readr)
        
        required_cols <- c("DATE", "TIME", "MPVPosition")
        all_needed_cols <- unique(c(required_cols, gas))
        
        appendData <- function() {
                dat_files <- list.files(path = input_path, recursive = TRUE, pattern = "\\.dat$", full.names = TRUE)
                total_files <- length(dat_files)
                data_frames <- list()
                
                for (i in seq_along(dat_files)) {
                        current_data <- read.table(dat_files[i], header = TRUE)
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
        
        # Create fixed interval bins (interval is in seconds, so no multiplication)
        time_bins <- seq(from = as.POSIXct(start_time, tz = "UTC"), 
                         to = as.POSIXct(end_time, tz = "UTC"), 
                         by = interval)
        
        merged_data <- merged_data %>%
                mutate(interval_bin = cut(DATE.TIME, breaks = time_bins, right = FALSE))
        
        # Step diagnostics
        interval_counts <- merged_data %>%
                group_by(interval_bin) %>%
                summarise(count = n()) %>%
                ungroup()
        
        abnormal_intervals <- interval_counts %>%
                filter(count == 0)
        
        cat("Step diagnostics:\n")
        cat("Total intervals:", length(time_bins) - 1, "\n")
        cat("Intervals with zero measurements (missing data):", nrow(abnormal_intervals), "\n")
        if (nrow(abnormal_intervals) > 0) {
                cat("Missing intervals:\n")
                print(abnormal_intervals)
        }
        
        # Apply flush logic per interval_bin and MPVPosition
        merged_data <- merged_data %>%
                group_by(interval_bin, MPVPosition) %>%
                arrange(DATE.TIME) %>%
                mutate(timestamp_for_step = last(DATE.TIME),
                       time_rank = row_number()) %>%
                mutate(across(all_of(gas), ~ if_else(time_rank <= flush, NA_real_, .))) %>%
                ungroup()
        
        # Summarise per interval_bin and MPVPosition, averaging up to interval points after flush
        summarized <- merged_data %>%
                group_by(interval_bin, MPVPosition) %>%
                arrange(DATE.TIME) %>%
                mutate(time_rank = row_number()) %>%
                filter(time_rank <= interval) %>%
                summarise(
                        DATE.TIME = max(timestamp_for_step),
                        across(all_of(gas), ~ mean(.x, na.rm = TRUE)),
                        .groups = "drop"
                )
        
        # Convert interval_bin factor to character start time for DATE.TIME column (ISO-like)
        summarized <- summarized %>%
                mutate(DATE.TIME = sub("\\[(.+),.*", "\\1", interval_bin)) %>%
                mutate(DATE.TIME = gsub(" ", "T", DATE.TIME)) %>%
                select(-interval_bin) %>%
                arrange(DATE.TIME, MPVPosition)
        
        # Add lab and analyzer columns
        summarized <- summarized %>%
                mutate(lab = lab,
                       analyzer = analyzer) %>%
                select(DATE.TIME, MPVPosition, lab, analyzer, everything())
        
        # Convert NH3 from ppm to mg/m3 if present
        if ("NH3" %in% colnames(summarized)) {
                summarized <- summarized %>%
                        mutate(NH3 = NH3 / 1000)
        }
        
        cat("Number of representative observations for each MPVPosition level:\n")
        print(table(summarized$MPVPosition))
        
        # Format start and end times for filename
        start_str <- format(as.POSIXct(start_time, tz = "UTC"), "%Y%m%d%H%M%S")
        end_str <- format(as.POSIXct(end_time, tz = "UTC"), "%Y%m%d%H%M%S")
        
        # interval in minutes for filename (seconds / 60)
        interval_min <- interval / 60
        
        file_name <- paste0(start_str, "-", end_str, "_", lab, "_", interval_min, "_avg_", analyzer, "_.csv")
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


