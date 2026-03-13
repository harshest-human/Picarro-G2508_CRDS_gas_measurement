
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
                arrange(DATE.TIME, var, analyzer) 
        
        df_long <- bind_rows(df_long, baseline_df) %>%
                arrange(DATE.TIME)
        
        return(df_long)
}

# Errorbarplot Function
emierrorbarplot <- emimeanse_plot <- function(data, y = NULL, location_filter = NULL, plot_err = FALSE) {
}

# Trend Plot Function 
emitrendplot <- function(data, y = NULL, location_filter = NULL, plot_err = FALSE, x = "DATE.TIME") {
}
