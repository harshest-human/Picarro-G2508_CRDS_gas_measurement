
# Development of indirect.CO2.balance function
indirect.CO2.balance <- function(df) {
        library(dplyr)
        
        ppm_to_mgm3 <- function(ppm, molar_mass) {
                T_K <- 273.15      # Kelvin
                P   <- 101325      # Pa
                R   <- 8.314472    # J/mol/K
                (ppm * 1e-6) * molar_mass * 1e3 * P / (R * T_K) # mg/m³
        }
        
        df %>%
                mutate(
                        hour = as.numeric(format(DATE.TIME, "%H")),
                        a = 0.22,
                        h_min = 2.9, # hour of minimal activity
                        
                        phi = 5.6 * m_weight^0.75 + 22 * Y1_milk_prod + 1.6e-5 * p_pregnancy_day^3,
                        t_factor = 1 + 4e-5 * (20 - temp_in)^3,
                        phi_T_cor = phi * t_factor,
                        A_cor = 1 - a * sin((2 * pi / 24) * (hour + 6 - h_min)),
                        hpu_T_A_cor_per_cow = phi_T_cor * A_cor,
                        
                        PCO2 = (0.185 * hpu_T_A_cor_per_cow) * 1000,
                        
                        delta_CO2_mgm3 = ppm_to_mgm3(delta_CO2, 44.01),
                        delta_NH3_mgm3 = ppm_to_mgm3(delta_NH3, 17.031),
                        delta_CH4_mgm3 = ppm_to_mgm3(delta_CH4, 16.04),
                        
                        Q_vent = PCO2 / delta_CO2_mgm3,
                        
                        # Instantaneous emissions (g/h) divided by 1000 to convert mg to g
                        e_NH3_gh = (delta_NH3_mgm3 * Q_vent / 1000) * n_dairycows,
                        e_CH4_gh = (delta_CH4_mgm3 * Q_vent / 1000) * n_dairycows,
                        
                        # Annual emissions (kg/year) divided by 1000 to convert g to kg
                        e_NH3_ghLU = (e_NH3_gh * 500) / (n_dairycows * m_weight),
                        e_CH4_ghLU = (e_CH4_gh * 500) / (n_dairycows * m_weight)
                )
}

# Development of pivot longer function
reshaper <- function(df) {
        library(dplyr)
        library(tidyr)
        library(stringr)
        library(lubridate)
        
        meta_cols <- c("DATE.TIME", "analyzer")
        
        # Identify numeric measurement columns
        measure_cols <- df %>%
                select(-all_of(meta_cols)) %>%
                select(where(is.numeric)) %>%
                names()
        
        # Pivot to long format
        df_long <- df %>%
                pivot_longer(
                        cols = all_of(measure_cols),
                        names_to = "var",
                        values_to = "value"
                ) %>%
                select(DATE.TIME, analyzer, var, value) %>%
                arrange(DATE.TIME, var, analyzer) %>%
                arrange(DATE.TIME)
        
        return(df_long)
}

# Errorbarplot Function
emierrorbarplot <- function(data, y = NULL) {
        library(dplyr)
        library(ggplot2)
        library(scales)
        
        if ("var" %in% names(data) && !"variable" %in% names(data)) {
                data <- data %>% rename(variable = var)
        }
        
        if (!is.null(y)) {
                data <- data %>% filter(variable %in% y)
        }
        
        if (!"value" %in% names(data)) stop("Column 'value' not found in data.")
        
        facet_labels <- c(
                "CO2_mgm3"    = "c[CO2]~'(mg '*m^-3*')'",
                "CH4_mgm3"    = "c[CH4]~'(mg '*m^-3*')'",
                "NH3_mgm3"    = "c[NH3]~'(mg '*m^-3*')'",
                "r_CH4/CO2"   = "c[CH4]/c[CO2]~'('*'%'*')'",
                "r_NH3/CO2"   = "c[NH3]/c[CO2]~'('*'%'*')'",
                "delta_CO2"   = "Delta*c[CO2]~'(mg '*m^-3*')'",
                "delta_CH4"   = "Delta*c[CH4]~'(mg '*m^-3*')'",
                "delta_NH3"   = "Delta*c[NH3]~'(mg '*m^-3*')'",
                "Q_vent"      = "Q~'('*m^3~h^-1~LU^-1*')'",
                "e_CH4_gh"    = "e[CH4]~'(g '*h^-1*')'",
                "e_NH3_gh"    = "e[NH3]~'(g '*h^-1*')'",
                "e_CH4_ghLU"  = "e[CH4]~'(g '*h^-1~LU^-1*')'",
                "e_NH3_ghLU"  = "e[NH3]~'(g '*h^-1~LU^-1*')'",
                "temp"        = "Temperature~(degree*C)",
                "RH"          = "Relative~Humidity~('%')",
                "ws_mst"      = "Wind~Speed~Mast~(m~s^-1)",
                "wd_mst"      = "Wind~Direction~Mast~(degree)",
                "wd_trv"      = "Wind~Direction~Traverse~(degree)",
                "ws_trv"      = "Wind~Speed~Traverse~(m~s^-1)",
                "n_dairycows" = "'Number of Cows'"
        )
        
        data <- data %>%
                mutate(
                        facet_label = factor(
                                facet_labels[as.character(variable)],
                                levels = facet_labels[y]
                        )
                )
        
        analyzer_colors <- c(
                "Cubic" = "#1b9e77", "Pronova" = "#7570b3", "Picarro" = "darkgray"
        )
        
        analyzer_shapes <- c(
                "Cubic" = 0, "Pronova" = 1, "Picarro" = 2 
        )
        
        all_analyzers <- unique(data$analyzer)
        analyzer_colors_full <- setNames(rep("black", length(all_analyzers)), all_analyzers)
        analyzer_shapes_full <- setNames(rep(16, length(all_analyzers)), all_analyzers)
        analyzer_colors_full[names(analyzer_colors)] <- analyzer_colors
        analyzer_shapes_full[names(analyzer_shapes)] <- analyzer_shapes
        
        summary_data <- data %>%
                group_by(analyzer, variable, facet_label) %>%
                summarise(
                        mean = mean(value, na.rm = TRUE),
                        sd   = sd(value, na.rm = TRUE),
                        n    = n(),
                        se   = sd / sqrt(n),
                        .groups = "drop"
                )
        
        p <- ggplot(summary_data, aes(x = analyzer, y = mean, color = analyzer, shape = analyzer)) +
                geom_point(position = position_dodge(width = 0.6), size = 3) +
                geom_errorbar(
                        aes(ymin = mean - se, ymax = mean + se),
                        width = 0.2,
                        position = position_dodge(width = 0.6)
                ) +
                scale_color_manual(values = analyzer_colors_full) +
                scale_shape_manual(values = analyzer_shapes_full) +
                facet_grid(facet_label ~ ., scales = "free_y", switch = "y",
                           labeller = labeller(facet_label = label_parsed)) +
                scale_y_continuous(
                        breaks = function(limits) seq(limits[1], limits[2], length.out = 6),
                        labels = scales::label_number(accuracy = 0.1, big.mark = "")
                ) +
                labs(x = NULL, y = NULL) +
                theme_classic() +
                theme(
                        text = element_text(size = 14),
                        axis.text = element_text(size = 14),
                        axis.title = element_text(size = 14),
                        axis.text.x = element_text(angle = 45, hjust = 1, size = 12),
                        axis.text.y = element_text(hjust = 1, size = 12),
                        strip.text.y.left = element_text(size = 14, vjust = 0.5),
                        panel.border = element_rect(color = "black", fill = NA),
                        legend.position = "bottom",
                        legend.title = element_blank(),
                        plot.title = element_text(hjust = 0.5)
                ) +
                guides(color = guide_legend(nrow = 1), shape = guide_legend(nrow = 1))
        
        print(p)
        return(p)
}

# Trend Plot Function 
emitrendplot <- function(data, y = NULL) {
        library(dplyr)
        library(ggplot2)
        library(scales)
        
        if ("var" %in% names(data) && !"variable" %in% names(data)) {
                data <- data %>% rename(variable = var)
        }
        
        if (!is.null(y)) {
                data <- data %>% filter(variable %in% y)
        }
        
        if (!"value" %in% names(data)) stop("Column 'value' not found in data.")
        if (!"DATE.TIME" %in% names(data)) stop("Column 'DATE.TIME' not found in data.")
        
        summary_data <- data %>%
                group_by(DATE.TIME, analyzer, variable) %>%
                summarise(
                        mean_val = mean(value, na.rm = TRUE),
                        sd_val   = sd(value, na.rm = TRUE),
                        .groups = "drop"
                ) %>%
                filter(!is.na(mean_val))
        
        if (nrow(summary_data) == 0) {
                stop("No data available for the selected variables.")
        }
        
        facet_labels <- c(
                "CO2_mgm3"    = "c[CO2]~'(mg '*m^-3*')'",
                "CH4_mgm3"    = "c[CH4]~'(mg '*m^-3*')'",
                "NH3_mgm3"    = "c[NH3]~'(mg '*m^-3*')'",
                "r_CH4/CO2"   = "c[CH4]/c[CO2]~'('*'%'*')'",
                "r_NH3/CO2"   = "c[NH3]/c[CO2]~'('*'%'*')'",
                "delta_CO2"   = "Delta*c[CO2]~'(mg '*m^-3*')'",
                "delta_CH4"   = "Delta*c[CH4]~'(mg '*m^-3*')'",
                "delta_NH3"   = "Delta*c[NH3]~'(mg '*m^-3*')'",
                "Q_vent"      = "Q~'('*m^3~h^-1~LU^-1*')'",
                "e_CH4_gh"    = "e[CH4]~'(g '*h^-1*')'",
                "e_NH3_gh"    = "e[NH3]~'(g '*h^-1*')'",
                "e_CH4_ghLU"  = "e[CH4]~'(g '*h^-1~LU^-1*')'",
                "e_NH3_ghLU"  = "e[NH3]~'(g '*h^-1~LU^-1*')'",
                "temp"        = "Temperature~(degree*C)",
                "RH"          = "Relative~Humidity~('%')",
                "ws_mst"      = "Wind~Speed~Mast~(m~s^-1)",
                "wd_mst"      = "Wind~Direction~Mast~(degree)",
                "wd_trv"      = "Wind~Direction~Traverse~(degree)",
                "ws_trv"      = "Wind~Speed~Traverse~(m~s^-1)",
                "n_dairycows" = "'Number of Cows'"
        )
        
        summary_data <- summary_data %>%
                mutate(
                        facet_label = factor(
                                facet_labels[as.character(variable)],
                                levels = facet_labels[y]
                        )
                )
        
        analyzer_colors <- c(
                "Cubic" = "#1b9e77", "Pronova" = "#7570b3", "Picarro" = "darkgray"
        )
        
        analyzer_shapes <- c(
                "Cubic" = 0, "Pronova" = 1, "Picarro" = 2 
        )
        
        all_analyzers <- unique(summary_data$analyzer)
        analyzer_colors_full <- setNames(rep("black", length(all_analyzers)), all_analyzers)
        analyzer_shapes_full <- setNames(rep(16, length(all_analyzers)), all_analyzers)
        analyzer_colors_full[names(analyzer_colors)] <- analyzer_colors
        analyzer_shapes_full[names(analyzer_shapes)] <- analyzer_shapes
        
        x_breaks <- seq(
                from = min(summary_data$DATE.TIME, na.rm = TRUE),
                to   = max(summary_data$DATE.TIME, na.rm = TRUE),
                by   = "12 hours"
        )
        
        p <- ggplot(
                summary_data,
                aes(x = DATE.TIME, y = mean_val, color = analyzer, shape = analyzer, group = analyzer)
        ) +
                geom_line(linewidth = 0.5, alpha = 0.6, na.rm = TRUE) +
                geom_point(size = 2, na.rm = TRUE) +
                geom_errorbar(
                        aes(ymin = mean_val - sd_val, ymax = mean_val + sd_val),
                        width = 0.4,
                        na.rm = TRUE
                ) +
                scale_color_manual(values = analyzer_colors_full) +
                scale_shape_manual(values = analyzer_shapes_full) +
                facet_grid(facet_label ~ ., scales = "free_y", switch = "y",
                           labeller = labeller(facet_label = label_parsed)) +
                scale_y_continuous(
                        breaks = scales::pretty_breaks(n = 6),
                        labels = scales::label_number(accuracy = 1, big.mark = "")
                ) +
                scale_x_datetime(breaks = x_breaks, date_labels = "%Y-%m-%d %H:%M") +
                labs(x = NULL, y = NULL) +
                theme_classic() +
                theme(
                        text = element_text(size = 14),
                        axis.text = element_text(size = 14),
                        axis.title = element_text(size = 14),
                        axis.text.x = element_text(angle = 45, hjust = 1, size = 8),
                        axis.text.y = element_text(hjust = 1, size = 12),
                        strip.text.y.left = element_text(size = 14, vjust = 0.5),
                        panel.border = element_rect(color = "black", fill = NA),
                        legend.position = "bottom",
                        legend.title = element_blank(),
                        plot.title = element_text(hjust = 0.5)
                ) +
                guides(color = guide_legend(nrow = 1))
        
        print(p)
        return(p)
}
