piclean <- function(input_path,
                    gas,
                    start_time,
                    end_time,
                    flush,
                    interval,
                    MPVPosition.levels = NULL,
                    location.levels = NULL,
                    lab = NULL,
                    analyzer = NULL) 
        {
        
        library(dplyr)
        library(lubridate)
        
        required_cols <- c("DATE", "TIME", "MPVPosition")
        all_needed_cols <- unique(c(required_cols, gas))
        
        # Step 1: Load all .dat files
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
        
        # Step 2: Merge date and time & filter early
        cat("Merging DATE and TIME column...\n")
        merged_data <- merged_data %>%
                mutate(DATE.TIME = as.POSIXct(paste(DATE, TIME), format = "%Y-%m-%d %H:%M:%S")) %>%
                select(-DATE, -TIME)
        
        if (!is.null(start_time) && !is.null(end_time)) {
                cat("Filtering data between:\n",
                    "Start:", start_time, "\n",
                    "End:  ", end_time, "\n")
                merged_data <- merged_data %>%
                        filter(DATE.TIME >= as.POSIXct(start_time),
                               DATE.TIME <= as.POSIXct(end_time))
        }
        
        # Step 3: Remove invalid MPVPositions
        cat("Removing non-integer and zero MPVPosition values...\n")
        merged_data <- merged_data %>%
                filter(!is.na(MPVPosition) & MPVPosition %% 1 == 0 & MPVPosition != 0) %>%
                arrange(DATE.TIME) %>%
                mutate(step_id = cumsum(c(1, diff(MPVPosition) != 0)))
        
        # Step 4: Apply MPVPosition & location levels if provided
        if (!is.null(MPVPosition.levels)) {
                merged_data$MPVPosition <- factor(merged_data$MPVPosition, levels = MPVPosition.levels)
        }
        if (!is.null(location.levels) && !is.null(MPVPosition.levels)) {
                merged_data$location <- factor(location.levels[merged_data$MPVPosition], levels = location.levels)
        }
        
        # Step 5: Flush out initial readings
        merged_data <- merged_data %>%
                group_by(step_id, MPVPosition) %>%
                arrange(DATE.TIME) %>%
                mutate(timestamp_for_step = last(DATE.TIME),
                       time_rank = row_number()) %>%
                mutate(across(all_of(gas), ~ if_else(time_rank <= flush, NA_real_, .)))
        
        # Step 6: Summarise by step_id with measuring.time
        summarized <- merged_data %>%
                group_by(step_id, MPVPosition, location) %>%
                arrange(DATE.TIME) %>%
                mutate(time_rank = row_number()) %>%
                filter(time_rank <= interval) %>%
                summarise(
                        DATE.TIME = last(timestamp_for_step),
                        measuring.time = sum(time_rank > flush & time_rank <= interval),
                        across(all_of(gas), ~ mean(.x, na.rm = TRUE)),
                        .groups = "drop")
        
        # Step 7: Convert units — always calculate ppm & mg/m3, remove original gas columns
        cat("Converting gas units...\n")
        summarized <- summarized %>%
                mutate(lab = lab, analyzer = analyzer,
                       CO2_ppm = if ("CO2" %in% colnames(.)) CO2 else NA_real_,
                       CH4_ppm = if ("CH4" %in% colnames(.)) CH4 else NA_real_,
                       NH3_ppm = if ("NH3" %in% colnames(.)) NH3 / 1000 else NA_real_,
                       H2O_vol = if ("H2O" %in% colnames(.)) H2O else NA_real_,
                       R = 8.314472,
                       T = 273.15,
                       P = 1013.25 * 100,
                       CO2_mgm3 = (CO2_ppm / 1000 * 44.01 * P) / (R * T),
                       CH4_mgm3 = (CH4_ppm / 1000 * 16.04 * P) / (R * T),
                       NH3_mgm3 = (NH3_ppm / 1000 * 17.031 * P) / (R * T)) %>%
                select(DATE.TIME, lab, analyzer, location, everything(),-all_of(gas), -R, -T, -P)
        
        # Step 8: Add lab & analyzer info
        cat("Adding lab and analyzer info...\n")
        if (!is.null(lab)) summarized$lab <- lab
        if (!is.null(analyzer)) summarized$analyzer <- analyzer
        
        # Step 9: Save output
        cat("Saving data to CSV...\n")
        start_str <- format(as.POSIXct(start_time), "%Y%m%d.%H%M")
        end_str <- format(as.POSIXct(end_time), "%Y%m%d.%H%M")
        file_name <- paste0(start_str, "-", end_str, "_", lab, "_", interval, "avg_", analyzer, ".csv")
        full_output_path <- file.path(getwd(), file_name)
        
        write.csv(summarized, full_output_path, row.names = FALSE, quote = FALSE)
        
        cat("✔ Data successfully processed and saved to:", full_output_path, "\n")
        cat("✔ Dataframe contains", ncol(summarized), "variables\n")
        
        return(summarized)
}
