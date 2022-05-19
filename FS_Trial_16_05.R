library(tidyverse)
test1 <- dir("trial_merge", full.names = T) %>% map_df(read_table, .id = NULL)

test1<- test1 %>% select(-42,-41,-40,-39)

test1<- test1 %>% select(DATE, TIME, MPVPosition,CO2, NH3, CH4)

test1<- test1 %>% filter(MPVPosition == as.integer(MPVPosition))

test1$MPVPosition <- as.factor(test1$MPVPosition)


#Box_Plots
plot(CO2~MPVPosition, data=test1, main = "MPVxCO2")

plot(CH4~MPVPosition, data=test1, main = "MPVxCH4")

plot(NH3~MPVPosition, data=test1, main = "MPVxNH3")

#ggline
ggline(test1, x="MPVPosition", y="CH4", na.rm=TRUE, add = "mean_se")
