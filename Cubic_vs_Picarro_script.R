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
CRDS <- read.csv("D:/Data Analysis/Gas_data/Raw_data/CRDS_raw/Picarro_G2509/2025/10/28/JFAADS2266-20251028-134016-DataLog_User.dat", sep="")
CRDS <- CRDS %>% select(DATE, TIME, MPVPosition, CH4, NH3, CO2) %>%
        mutate(DATE.TIME = ymd_hms(paste(DATE, TIME)),analyzer = "CRDS9") %>%
        filter(DATE.TIME >= ymd_hms("2025-10-28 13:55:58"),
               DATE.TIME <= ymd_hms("2025-10-28 14:10:55")) %>%
        select(-"DATE", -"TIME")
        
CUBIC <- 
