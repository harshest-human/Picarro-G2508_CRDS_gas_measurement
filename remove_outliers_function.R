remove_outliers <- function(df, exclude_cols = NULL, group_cols = NULL) {
        # Helper function for a single numeric vector
        remove_vec_outliers <- function(x) {
                qnt <- quantile(x, probs = c(0.25, 0.75), na.rm = TRUE)
                H <- 1.5 * IQR(x, na.rm = TRUE)
                x[x < (qnt[1] - H) | x > (qnt[2] + H)] <- NA
                return(x)
        }
        
        # Columns to process (numeric and not excluded)
        cols_to_process <- names(df)[sapply(df, is.numeric)]
        if (!is.null(exclude_cols)) {
                cols_to_process <- setdiff(cols_to_process, exclude_cols)
        }
        
        # Before counts
        before_counts <- colSums(!is.na(df[cols_to_process]))
        
        # Apply outlier removal
        if (!is.null(group_cols)) {
                df <- df %>%
                        group_by(across(all_of(group_cols))) %>%
                        mutate(across(all_of(cols_to_process), remove_vec_outliers)) %>%
                        ungroup()
        } else {
                df[cols_to_process] <- lapply(df[cols_to_process], remove_vec_outliers)
        }
        
        # After counts
        after_counts <- colSums(!is.na(df[cols_to_process]))
        
        # Print summary (# removed)
        removed_counts <- before_counts - after_counts
        print(data.frame(Variable = names(removed_counts),
                         Removed = removed_counts))
        
        return(df)
}
