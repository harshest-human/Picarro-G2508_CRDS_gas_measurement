getwd()
library(tidyverse)
library(reshape2)
library(hablar)
library(lubridate)
library(psych)
library(rmarkdown)
library(ggplot2)
library(readxl)
library(dplyr)

input <- read.table("picarro14july21PVCSETUP8X2.dat", header = T) %>% 
        select(DATE, TIME,MPVPosition, N2O, CO2, CH4_dry, H2O, NH3) %>%
        filter(MPVPosition == as.integer(MPVPosition), DATE >= "2021-07-15") %>%
        na.omit("MPVPosition")
input$DATE <- as.Date(input$DATE)


#1 MPVxCO2 New Line (After  milking parlour)
MPVxCO2ii <- input %>% select(MPVPosition, CO2) %>%
        filter(MPVPosition <= 8, MPVPosition >=1, CO2 >=0) %>% convert(fct(MPVPosition))

CO2linregrii <- lm(CO2~MPVPosition, data=MPVxCO2ii)
summary(CO2linregrii)    
plot(CO2~MPVPosition, data=MPVxCO2ii, main = "MPVxCO2 (ii)")

#1 MPVxCO2 Old Line (before  milking parlour)
MPVxCO2i <- input %>% select(MPVPosition, CO2) %>%
        filter(MPVPosition <= 16, MPVPosition >=9, CO2 >=0) %>% convert(fct(MPVPosition))

CO2linregri <- lm(CO2~MPVPosition, data=MPVxCO2i)
summary(CO2linregri)
plot(CO2~MPVPosition, data=MPVxCO2i, main = "MPVxCO2 (i)")

anova(CO2linregrii)


#2 MPVxCH4_dry New Line (After  milking parlour)
MPVxCH4_dryii <- input %>% select(MPVPosition, CH4_dry) %>%
        filter(MPVPosition <= 8, MPVPosition >=1, CH4_dry >=0) %>% convert(fct(MPVPosition))

CH4_drylinregrii <- lm(CH4_dry~MPVPosition, data=MPVxCH4_dryii)
summary(CH4_drylinregrii)
plot(CH4_dry~MPVPosition, data=MPVxCH4_dryii, main = "MPVxCH4_dry (ii)")

#2 MPVxCH4_dry Old Line (before  milking parlour)
MPVxCH4_dryi <- input %>% select(MPVPosition, CH4_dry) %>%
        filter(MPVPosition <= 16, MPVPosition >=9, CH4_dry >=0) %>% convert(fct(MPVPosition))

CH4_drylinregri <- lm(CH4_dry~MPVPosition, data=MPVxCH4_dryi)
summary(CH4_drylinregri)
plot(CH4_dry~MPVPosition, data=MPVxCH4_dryi, main = "MPVxCH4_dry (i)")



#3 MPVxNH3 New Line (After  milking parlour)
MPVxNH3ii <- input %>% select(MPVPosition, NH3) %>%
        filter(MPVPosition <= 8, MPVPosition >=1) %>% convert(fct(MPVPosition))

NH3linregrii <- lm(NH3~MPVPosition, data=MPVxNH3ii)
summary(NH3linregrii)
plot(NH3~MPVPosition, data=MPVxNH3ii, main = "MPVxNH3 (ii)")

#3 MPVxNH3 Old Line (before  milking parlour)
MPVxNH3i <- input %>% select(MPVPosition, NH3) %>%
        filter(MPVPosition <= 16, MPVPosition >=9) %>% convert(fct(MPVPosition))

NH3linregri <- lm(NH3~MPVPosition, data=MPVxNH3i)
summary(NH3linregr)
plot(NH3~MPVPosition, data=MPVxNH3i, main = "MPVxNH3 (i)")




# T-test
MPVxCO2i %>% group_by(MPVPosition) %>% summarise(check = mean(CO2)) #mean tibble
t.test(MPVxCO2i[MPVxCO2i$MPVPosition==9, 2], MPVxCO2i[MPVxCO2i$MPVPosition==16,2 ])

MPVxCO2ii %>% group_by(MPVPosition) %>% summarise(check = mean(CO2)) #mean tibble
t.test(MPVxCO2ii[MPVxCO2ii$MPVPosition==1, 2], MPVxCO2ii[MPVxCO2ii$MPVPosition==8,2 ])


MPVxCH4_dryi %>% group_by(MPVPosition) %>% summarise(check = mean(CH4_dry)) #mean tibble

t.test(MPVxCH4_dryi[MPVxCH4_dryi$MPVPosition==2, 2], MPVxCH4_dryi[MPVxCH4_dryi$MPVPosition==3,2 ])



MPVxNH3i %>% group_by(MPVPosition) %>% summarise(check = mean(NH3)) #mean tibble

t.test(MPVxNH3i[MPVxNH3i$MPVPosition==15, 2], MPVxNH3i[MPVxNH3i$MPVPosition==16,2 ])




