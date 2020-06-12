# Data source: https://www.followthemoney.org/

library(tidyverse)
library(janitor)
library(lubridate)


# function to clean and format the FTM files
process_ftm_table <- function(rawdata){
  #clean columns and remove :id and :token fields
  cleandata <- rawdata %>% 
    select(!ends_with(c(":id", ":token")), -request) %>% 
    clean_names() %>% 
    rename(dollar_amount = total) %>% 
    #format number columns
    mutate(
      number_of_records = as.integer(number_of_records),
      dollar_amount = as.numeric(dollar_amount)
    ) %>% 
    #filter for just organizations
    filter(type_of_contributor == "Non-Individual")
  return(cleandata)
  }


### PARTY COMMITTEES ####

#import dataset on contribs to party committees
raw_contribs_topartycmtes <- read_csv("raw_data/FTM_pu_contributions_to_partycmtes.csv", col_types = cols(.default = "c"))

#run the function
contribs_topartycmtes <- process_ftm_table(raw_contribs_topartycmtes)

#save results to file
saveRDS(contribs_topartycmtes, "processed_data/contribs_topartycmtes.rds")
write_csv(contribs_topartycmtes, "processed_data/contribs_topartycmtes.csv")

#total money?
contribs_topartycmtes %>% 
  summarise(sum(dollar_amount))




### CANDIDATES ####

raw_contribs_tocands <- read_csv("raw_data/FTM_pu_contributions_to_candidates.csv", col_types = cols(.default = "c"))

#run the function
contribs_tocands <- process_ftm_table(raw_contribs_tocands)

#save results to file
saveRDS(contribs_tocands, "processed_data/contribs_tocands.rds")
write_csv(contribs_tocands, "processed_data/contribs_tocands.csv")

#total money?
contribs_tocands %>% 
  summarise(sum(dollar_amount))



### BALLOT MEASURES ####

raw_contribs_toballotmeasures <- read_csv("raw_data/FTM_pu_contributions_to_ballotmeasures.csv", col_types = cols(.default = "c"))

#run the function
contribs_toballotmeasures <- process_ftm_table(raw_contribs_toballotmeasures)

#save results to file
saveRDS(contribs_toballotmeasures, "processed_data/contribs_toballotmeasures.rds")
write_csv(contribs_toballotmeasures, "processed_data/contribs_toballotmeasures.csv")

#total money?
contribs_toballotmeasures %>% 
  summarise(sum(dollar_amount))



### *CURRENT* OFFICEHOLDERS ####
#donors by party only here

raw_currentofficeholders_byparty <- read_csv("raw_data/FTM_pu_to_allcurrentofficerholders_byparty.csv", col_types = cols(.default = "c"))

#run the function
currentofficeholders_byparty <- process_ftm_table(raw_currentofficeholders_byparty)

#save results to file
saveRDS(currentofficeholders_byparty, "processed_data/currentofficeholders_byparty.rds")
write_csv(currentofficeholders_byparty, "processed_data/currentofficeholders_byparty.csv")

#total money?
currentofficeholders_byparty %>% 
  summarise(sum(dollar_amount))



### GRAND TOTALS FOR CANDIDATES - FROM ALL DONORS ####
#these grand totals can be used to calculate percentages for union of a race if desired

raw_grandtotals_alldonors_tocandidates <- read_csv("raw_data/FTM_grandtotals_alldonors_tocandidates.csv", col_types = cols(.default = "c"))

#earlier function won't work because missing column here. So do it directly instead
grandtotals_alldonors_tocandidates <- raw_grandtotals_alldonors_tocandidates %>% 
  select(!ends_with(c(":id", ":token")), -request) %>% 
  clean_names() %>% 
  rename(dollar_amount = total) %>% 
  #format number columns
  mutate(
    number_of_records = as.integer(number_of_records),
    dollar_amount = as.numeric(dollar_amount)
  ) 

#save results to file
saveRDS(grandtotals_alldonors_tocandidates, "processed_data/grandtotals_alldonors_tocandidates.rds")
write_csv(grandtotals_alldonors_tocandidates, "processed_data/grandtotals_alldonors_tocandidates.csv")

#total money?
grandtotals_alldonors_tocandidates %>% 
  summarise(sum(dollar_amount))



### *TRANSACTION-LEVEL* DATA ON CONTRIBS TO CANDIDATES ####
#this level of detail less needed for this analysis but just in case

raw_transactionlevel_to_candidates <- read_csv("raw_data/FTM_transactionlevel_to_candidates.csv", col_types = cols(.default = "c"))

glimpse(raw_transactionlevel_to_candidates)

#earlier function won't work because different columns here. So do it directly instead
transactionlevel_to_candidates <- raw_transactionlevel_to_candidates %>% 
  select(!ends_with(c(":id", ":token")), -request) %>% 
  clean_names() %>% 
  rename(dollar_amount = amount) %>% 
  #format columns
  mutate(
    dollar_amount = as.numeric(dollar_amount),
    date = ymd(date)
  ) 

#save results to file
saveRDS(transactionlevel_to_candidates, "processed_data/transactionlevel_to_candidates.rds")

#total money?
transactionlevel_to_candidates %>% 
  summarise(sum(dollar_amount))



### CURRENT OFFICEHOLDERS - AGGREGATE STATE AND PARTY ####

raw_currentoffice_agg_state_party <- read_csv("raw_data/FTM_pu_currentofficeholders_agg_state_party.csv")

#earlier function won't work because different columns here. So do it directly instead
currentoffice_agg_state_party <- raw_currentoffice_agg_state_party %>% 
  select(!ends_with(c(":id", ":token")), -request) %>% 
  clean_names() %>% 
  rename(dollar_amount = total) %>% 
  #format columns
  mutate(
    dollar_amount = as.numeric(dollar_amount),
    number_of_records = as.integer(number_of_records)
  ) 

#save results to file
saveRDS(currentoffice_agg_state_party, "processed_data/currentoffice_agg_state_party.rds")

#total money?
currentoffice_agg_state_party %>% 
  summarise(sum(dollar_amount))




#### FILTERING OUT SOME UNIONS ####

#Emergency Responder categories sometimes includes police, sometimes just fire? Let's take a look
e1 <- currentofficeholders_byparty %>% 
  filter(specific_business == "Emergency responder unions and associations") 

e2 <- contribs_tocands %>% 
  filter(specific_business == "Emergency responder unions and associations") 

e3 <- contribs_topartycmtes %>% 
  filter(specific_business == "Emergency responder unions and associations") 

e4 <- contribs_toballotmeasures %>% 
  filter(specific_business == "Emergency responder unions and associations") 

e_all <- bind_rows(e1, e2, e3, e4) 

e_distinct <- e_all %>% 
            distinct(contributor)

e_distinct %>%
  write_csv("processed_data/firstrespondlist.csv")

