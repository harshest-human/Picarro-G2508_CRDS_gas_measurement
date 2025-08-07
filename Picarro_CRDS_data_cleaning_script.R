
################# Development of piclean function #########################
piclean <- function(input_path, gas, start_time, end_time, flush, interval, MPVPosition.levels, location.levels = NULL, lab, analyzer) {
        library(dplyr)
        library(lubridate)
        library(tidyr)
        
        # Step 1: Read and merge data
        required_cols <- c("DATE", "TIME", "MPVPosition")
        all_needed_cols <- unique(c(required_cols, gas))
        
        dat_files <- list.files(path = input_path, recursive = TRUE, pattern = "\\.dat$", full.names = TRUE)
        data_frames <- lapply(dat_files, function(f) {
                df <- read.table(f, header = TRUE)
                existing_cols <- all_needed_cols[all_needed_cols %in% colnames(df)]
                if(length(existing_cols) == 0) return(NULL)
                df[, existing_cols, drop=FALSE]
        })
        data_frames <- Filter(Negate(is.null), data_frames)
        if(length(data_frames) == 0) stop("No data frames found with matching columns.")
        
        merged_data <- do.call(rbind, data_frames)
        
        # Step 2: Create POSIXct datetime and filter by start/end time
        merged_data <- merged_data %>%
                mutate(DATE.TIME = as.POSIXct(paste(DATE, TIME), format="%Y-%m-%d %H:%M:%S", tz = "UTC")) %>%
                select(-DATE, -TIME) %>%
                filter(DATE.TIME >= as.POSIXct(start_time, tz = "UTC"),
                       DATE.TIME <= as.POSIXct(end_time, tz = "UTC"))
        
        # Step 3: Create full sequence of 1-sec timestamps
        full_time_seq <- tibble(DATE.TIME = seq(from = min(merged_data$DATE.TIME),
                                                to = max(merged_data$DATE.TIME),
                                                by = "1 sec"))
        
        # Step 4: Join original data to full time sequence
        merged_full <- full_time_seq %>%
                left_join(merged_data, by = "DATE.TIME")
        
        # Step 5: Identify invalid MPVPositions (0 or non-integer)
        invalid_mpv <- is.na(merged_full$MPVPosition) | merged_full$MPVPosition == 0 | (merged_full$MPVPosition %% 1 != 0)
        
        # Step 6: Fill entire rows with NA where MPVPosition invalid
        merged_full[invalid_mpv, c("MPVPosition", gas)] <- NA
        
        # Step 7: Create stepid grouping
        merged_full <- merged_full %>%
                arrange(DATE.TIME) %>%
                mutate(
                        valid_pos = !is.na(MPVPosition),
                        step_change = (valid_pos & (MPVPosition != lag(MPVPosition) | !lag(valid_pos, default=FALSE))) | (!valid_pos),
                        stepid = cumsum(step_change)
                ) %>%
                filter(!is.na(MPVPosition))
        
        # Step 8: Add secondkey counting seconds per step
        merged_full <- merged_full %>%
                group_by(stepid) %>%
                arrange(DATE.TIME) %>%
                mutate(secondkey = row_number()) %>%
                ungroup()
        
        # Step 9: Check step length deviations
        step_counts <- merged_full %>% 
                group_by(stepid) %>% 
                summarise(n_rows = n(), .groups = "drop")
        
        lower_bound <- interval * 0.5
        upper_bound <- interval * 1.5
        unusual_steps <- step_counts %>% filter(n_rows < lower_bound | n_rows > upper_bound)
        if(nrow(unusual_steps) > 0) {
                warning("Some steps have row counts deviating more than ±50% from expected interval:")
                print(unusual_steps)
        }
        
        # Step 10: Filter by interval and flush seconds
        merged_filtered <- merged_full %>%
                filter(secondkey <= interval, secondkey > flush)
        
        # Step 11: Summarize averages by stepid and MPVPosition
        summarized <- merged_filtered %>%
                group_by(stepid, MPVPosition) %>%
                summarise(
                        DATE.TIME = max(DATE.TIME),
                        across(all_of(gas), ~ mean(.x, na.rm = TRUE)),
                        .groups = "drop"
                ) %>%
                mutate(
                        lab = lab,
                        analyzer = analyzer,
                        DATE.TIME = format(DATE.TIME, "%Y-%m-%d %H:%M:%S")  # keep as character, not POSIXct
                )
        
        # Step 12: Optionally assign location if location.levels provided
        if (!is.null(location.levels)) {
                if(length(location.levels) != length(MPVPosition.levels)) {
                        stop("location.levels and MPVPosition.levels must be the same length.")
                }
                lookup_df <- tibble(MPVPosition = MPVPosition.levels, location = location.levels)
                summarized <- summarized %>%
                        left_join(lookup_df, by = "MPVPosition")
                
                # Step 13: Check MPVPosition sequence vs planned
                observed_seq <- summarized %>% arrange(DATE.TIME) %>% pull(MPVPosition)
                planned_pos_str <- paste(MPVPosition.levels, collapse = "-")
                observed_pos_str <- paste(unique(observed_seq), collapse = "-")
                
                if (planned_pos_str != observed_pos_str) {
                        warning("Observed MPVPosition sequence differs from planned sequence:\n",
                                "Planned: ", planned_pos_str, "\nObserved:", observed_pos_str)
                }
        }
        
        # Step 14: Adjust NH3 units if needed
        if("NH3" %in% gas && "NH3" %in% colnames(summarized)) {
                summarized <- summarized %>% mutate(NH3 = NH3 / 1000)
        }
        
        # Step 15: Print representative counts
        cat("Representative observations per MPVPosition:\n")
        print(table(summarized$MPVPosition))
        
        # Step 16: Export data (filename without analyzer or lab)
        start_str <- format(as.POSIXct(start_time, tz = "UTC"), "%Y%m%d%H%M%S")
        end_str <- format(as.POSIXct(end_time, tz = "UTC"), "%Y%m%d%H%M%S")
        file_name <- paste0(start_str, "-", end_str, "_", interval, "sec_gas_.csv")
        full_output_path <- file.path(getwd(), file_name)
        
        cat("Exporting summarized data to:\n", full_output_path, "\n")
        write.csv(summarized, full_output_path, row.names = FALSE, quote = FALSE)
        
        return(summarized)
}



