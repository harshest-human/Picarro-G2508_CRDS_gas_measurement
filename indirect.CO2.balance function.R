# Development of indirect.CO2.balance function
indirect.CO2.balance <- function(df) {
        library(dplyr)
        
        # ppm → mg/m³ at 0°C (273.15K) and 1 atm
        ppm_to_mgm3 <- function(ppm, molar_mass) {
                T_K <- 273.15      # Kelvin
                P   <- 101325      # Pa
                R   <- 8.314472    # J/mol/K
                (ppm * 1e-6) * molar_mass * 1e3 * P / (R * T_K)  # mg/m³
        }
        
        df <- df %>%
                mutate(
                        # Hour of day
                        hour = as.numeric(format(DATE.TIME, "%H")),
                        
                        # Animal activity constants
                        a = 0.22,
                        h_min = 2.9,  # hour of minimal activity
                        
                        # Baseline heat production per cow (W)
                        phi = 5.6 * m_weight^0.75 + 22 * Y1_milk_prod + 1.6e-5 * p_pregnancy_day^3,
                        
                        # Temperature correction factor
                        t_factor = 1 + 4e-5 * (20 - temp_in)^3,
                        phi_T_cor = phi * t_factor,
                        
                        # Relative animal activity correction
                        A_cor = 1 - a * (sin((2*pi/24) * (hour + 6 - h_min))),
                        
                        # Heat production per cow corrected for T and activity
                        hpu_T_A_cor_per_cow = phi_T_cor * A_cor,
                        
                        #PCO2
                        PCO2 = (0.185 * hpu_T_A_cor_per_cow) * 1000,
                        
                        # Convert CO2, NH3, CH4 from ppm → mg/m³ (0°C)
                        CO2_mgm3_in = ppm_to_mgm3(CO2_ppm_in, 44.01),
                        CO2_mgm3_N  = ppm_to_mgm3(CO2_ppm_N,  44.01),
                        CO2_mgm3_S  = ppm_to_mgm3(CO2_ppm_S,  44.01),
                        
                        NH3_mgm3_in = ppm_to_mgm3(NH3_ppm_in, 17.031),
                        NH3_mgm3_N  = ppm_to_mgm3(NH3_ppm_N,  17.031),
                        NH3_mgm3_S  = ppm_to_mgm3(NH3_ppm_S,  17.031),
                        
                        CH4_mgm3_in = ppm_to_mgm3(CH4_ppm_in, 16.04),
                        CH4_mgm3_N  = ppm_to_mgm3(CH4_ppm_N,  16.04),
                        CH4_mgm3_S  = ppm_to_mgm3(CH4_ppm_S,  16.04),
                        
                        # Delta concentrations (inside - outside)
                        delta_CO2_N = CO2_mgm3_in - CO2_mgm3_N,
                        delta_CO2_S = CO2_mgm3_in - CO2_mgm3_S,
                        
                        delta_NH3_N = NH3_mgm3_in - NH3_mgm3_N,
                        delta_NH3_S = NH3_mgm3_in - NH3_mgm3_S,
                        
                        delta_CH4_N = CH4_mgm3_in - CH4_mgm3_N,
                        delta_CH4_S = CH4_mgm3_in - CH4_mgm3_S,
                        
                        # Ventilation rate (m³/h)
                        Q_vent_N = ifelse(delta_CO2_N != 0, PCO2 / delta_CO2_N, NA_real_),
                        Q_vent_S = ifelse(delta_CO2_S != 0, PCO2 / delta_CO2_S, NA_real_),
                        
                        # Instantaneous emissions (g/h) divided by 1000 to convert mg to g
                        e_NH3_gh_N = (delta_NH3_N * Q_vent_N / 1000) * n_dairycows_in, 
                        e_CH4_gh_N = (delta_CH4_N * Q_vent_N / 1000) * n_dairycows_in,
                        e_NH3_gh_S = (delta_NH3_S * Q_vent_S / 1000) * n_dairycows_in,
                        e_CH4_gh_S = (delta_CH4_S * Q_vent_S / 1000) * n_dairycows_in,
                        
                        # Annual emissions (kg/year) divided by 1000 to convert g to kg
                        e_NH3_ghLU_N = (e_NH3_gh_N * 500) / (n_dairycows_in * m_weight),
                        e_CH4_ghLU_N = (e_CH4_gh_N * 500) / (n_dairycows_in * m_weight),
                        e_NH3_ghLU_S = (e_NH3_gh_S * 500) / (n_dairycows_in * m_weight),
                        e_CH4_ghLU_S = (e_CH4_gh_S * 500) / (n_dairycows_in * m_weight)
                )
        
        # Calculate CH4/CO2 and NH3/CO2 ratios (in %)
        gases_to_compare <- c("CH4", "NH3")
        locations <- c("in", "N", "S")
        
        for (gas in gases_to_compare) {
                for (loc in locations) {
                        col_gas <- paste0(gas, "_mgm3_", loc)
                        col_CO2 <- paste0("CO2_mgm3_", loc)
                        ratio_col <- paste0("r_", gas, "/CO2_", loc)
                        
                        df <- df %>%
                                mutate(
                                        !!ratio_col := ifelse(.data[[col_CO2]] != 0,
                                                              .data[[col_gas]] / .data[[col_CO2]] * 100,
                                                              NA_real_)
                                )
                }
        }
        
        return(df)
}