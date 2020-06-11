library(RODBC)
library(dplyr)
library(tidyverse)
library(lubridate)
library(janitor)
library(dbplyr)
library(readxl)
library(writexl)
library(RPostgreSQL)
options(scipen = 999) #no scientific notation

# Connecting to RPostgreSQL server

drv <- dbDriver('PostgreSQL')  
db <- 'news'  
host_db <- 'cnbdevx-cabds501'  
db_port <- '5432'  
db_user <- Sys.getenv("FEC_DB_USER") #set this value in .Renviron
db_password <- Sys.getenv("FEC_DB_PASSWORD") #set this value in .Renviron

con <- dbConnect(drv, dbname=db, host=host_db, port=db_port, user=db_user, password=db_password)

# remove user and pw info from the environment
rm(db_user)
rm(db_port)
rm(db_password)

#list the tables in the database to confirm connection
dbplyr::src_dbi(con)


# INDIVIDUAL CONTRIBS TABLE #### 

#connect to table with individual contributions (within fec schema)
contribs_db <- tbl(con, in_schema("fec", "individual_contributions"))

glimpse(contribs_db)

#how many cycles?
#make take a while to run
contribs_db %>% 
  count(cycle)


#uppercase the target fields
contribs_db <- contribs_db %>% 
  mutate(
    occupation = str_trim(str_to_upper(occupation)),
    employer = str_trim(str_to_upper(employer))
  )


#create sql statement to feed match terms to the database
sql_findpolice <- "SELECT *
	      FROM fec.individual_contributions
	      WHERE occupation LIKE '%POLICE%' OR
		          employer LIKE '%POLICE%' OR
		          occupation LIKE '%SHERIFF%' OR
		          employer LIKE '%SHERIFF%' OR
		          occupation LIKE '%SHERIIFF%' OR
		          employer LIKE '%SHERIIFF%' OR
		          occupation LIKE '%SHERIF%' OR
		          employer LIKE '%SHERIF%' OR
		          occupation LIKE '%LAW ENFORCE%' OR
		          employer LIKE '%LAW ENFORCE%' OR
		          occupation LIKE '%ENFORCEMENT OFFICER%' OR
		          employer LIKE '%ENFORCEMENT OFFICER%' OR
		          occupation LIKE '%DETECTIVE%' OR
		          employer LIKE '%DETECTIVE%'
              "

#run the query 
#this could take a while
matchresults_all <- dbGetQuery(con, sql_findpolice)

#convert to tibble for easier working with
matchresults_all <- as_tibble(matchresults_all)

#let's see what we have
head(matchresults_all)

#save results
saveRDS(matchresults_all, "processed_data/matchresults_all.rds")
write_csv(matchresults_all, "processed_data/matchresults_all.csv")



### EXPLORING CATEGORY VARIATIONS ####

#quick counts
matchresults_all %>% 
  count(employer, sort = TRUE)

matchresults_all %>% 
  count(occupation, sort = TRUE)

matchresults_all %>% 
  count(occupation, employer) %>% 
  arrange(occupation, desc(n))


#add a five-digit zip column, remove punctuation from employer/occupation
matchresults_all <- matchresults_all %>% 
  mutate(
    zip5 = str_sub(zip_code, 1L, 5L),
    employer = str_squish(gsub("[[:punct:]]", "", employer)),
    occupation = str_squish(gsub("[[:punct:]]", "", occupation))
  ) 

#create unique ID string for each donor
matchresults_all <- matchresults_all %>% 
    mutate(
      nametemp = str_squish(str_remove_all(name, ",")),
      nametemp = str_squish(str_remove_all(nametemp, " ")),
      uniqueidstring = str_to_upper(str_squish(str_c(nametemp, state, zip5))),
      uniqueidstring = str_squish(gsub("[[:punct:]]", "", uniqueidstring))
      )%>% 
    select(-nametemp)

#visually inspect to see if things look as expected                            
matchresults_all$uniqueidstring                             

#do we have any non-individual entity types included?
matchresults_all %>% 
  count(entity_tp)

matchresults_all %>% 
  filter(entity_tp != "IND") 
#looks like a small number of candidate donors who themselves are cops

#how many unique variations of occupation/employer
matchresults_all %>% 
  select(employer, occupation) %>% 
  distinct()


#create a table to export out for performing cleaning/standardization - determining if should be included as police 
#we'll use the unique id string to match them back up again after
#note: some donors list their employer/occuption differently in different records. This would collapse them into one,
#but seems reasonable to do so under the cirumstances to aid in manual inspection.
matchresults_lookuptable <- matchresults_all %>% 
  select(uniqueidstring, employer, occupation) %>% 
  distinct(uniqueidstring, .keep_all = TRUE)

head(matchresults_lookuptable)

#save to file
write_csv(matchresults_lookuptable, "processed_data/matchresults_lookuptable_orig.csv")



#### BRINGING RESULTS BACK IN AFTER CLEANING OUTSIDE OF R #####

#import file where cleaning was done separately
matchresults_lookuptable_clean <- read_excel("processed_data/matchresults_lookuptable_clean2.xlsx", 
                                              col_types = c("skip", "text", "text", 
                                                            "text", "text", "text", "text"))

matchresults_lookuptable_clean <- matchresults_lookuptable_clean %>% 
  select(
    uniqueidstring,
    police_flag = flag,
    employer_cleaned,
    occupation_cleaned
  ) 

matchresults_curated <- inner_join(matchresults_all, matchresults_lookuptable_clean, by = "uniqueidstring") 

#are there any that aren't joining?
anti_join(matchresults_all, matchresults_lookuptable_clean, by = "uniqueidstring")
#Hmmm.... that's a lot, need to find out what's going on here 
#Something must be happening with the id strings, need to figure out what

# ***to be continued later***
#****************************
#****************************

# for now we'll skip to the next step

matchresults_curated %>% 
  count(police_flag)

#we'll take the yes and maybe ones for now (later will sift more and then ultimately yes only filtered)
matchresults_curated <- matchresults_curated %>% 
  filter(police_flag %in% c("y", "m"))



#### BRINGING IN COMMITTEE LOOKUP TABLE #####

#create sql statement that joins relevant columns from cmte master and cand master
sql_cmtelookup <- "SELECT b.cmte_id, b.cand_id, b.cmte_tp, b.cmte_pty_affiliation, b.cmte_nm, 
                      a.cand_name, a.cand_election_yr, a.cand_office, a.cand_office_st, a.cand_office_district 
                	 FROM fec.committee_master b
                	 LEFT JOIN fec.candidate_master a ON a.cand_id = b.cand_id
                  "

#run the query to pull the data from server into local dataframe
committees <- dbGetQuery(con, sql_cmtelookup)

#convert to tibble format
committees <- as_tibble(committees)

#filter to just Democratic and Republican and find distinct
committees <- committees %>% 
  filter(cmte_pty_affiliation %in% c("DEM", "REP")) %>% 
  distinct(cmte_id, .keep_all = TRUE)

#check that are no duplicate cmte ids
committees %>% 
  count(cmte_id) %>% 
  filter(n > 1)


#join our committee lookup back to the main contribs table
matchresults_joined <- inner_join(matchresults_curated, committees, by = "cmte_id")

#let's take a look 
matchresults_joined




##### CONTRIBUTIONS ANALYSIS BY PARTY #############


#starting with grand totals

#dollars by party
matchresults_joined %>% 
  group_by(cmte_pty_affiliation) %>% 
  summarise(totdollars = sum(transaction_amt))

# dollars by party, cycle
by_party_cycle_all <- matchresults_joined %>% 
  group_by(cycle, cmte_pty_affiliation) %>% 
  summarise(totdollars = sum(transaction_amt)) %>% 
  ungroup()

by_party_cycle_all

#plot it 
#create function to use for all such plots below
func_plot_party_cycle <- function(mydata){
  
  mypallette <- c("#56B4E9", "#D55E00")
  
  g <- ggplot(mydata, aes(x = cycle, y = totdollars, fill = cmte_pty_affiliation)) +
    geom_col(position = "dodge") +
    scale_fill_manual(values = mypallette) +
    theme_minimal()  
  
  return(g)
}

#run the function 
func_plot_party_cycle(by_party_cycle_all)


#let's look just at contribs to candiates
by_party_cycle_cands <- matchresults_joined %>% 
  filter(!is.na(cand_id)) %>% 
  group_by(cycle, cmte_pty_affiliation) %>% 
  summarise(totdollars = sum(transaction_amt)) %>% 
  ungroup()

by_party_cycle_cands

func_plot_party_cycle(by_party_cycle_cands)



#just PRESIDENTIAL candidates
by_party_cycle_prezonly <- matchresults_joined %>% 
  filter(cand_office == "P") %>% 
  group_by(cycle, cmte_pty_affiliation) %>% 
  summarise(totdollars = sum(transaction_amt)) %>% 
  ungroup()

by_party_cycle_prezonly

func_plot_party_cycle(by_party_cycle_prezonly)
#NOTE: This looks like the Dems are blowing out Trump this cycle, but Trump has joint fundraising setup with the RNC
#and his super pacs which aren't included here. So may need to figure out how to account for these other funds.
#May want to isolate just the committees associated with Trump and the presidential Dems, use those specific cmte ids.
#The joint fundraising may complicate matters a bit, but still.


#just SENATE candidates
by_party_cycle_senate <- matchresults_joined %>% 
  filter(cand_office == "S") %>% 
  group_by(cycle, cmte_pty_affiliation) %>% 
  summarise(totdollars = sum(transaction_amt)) %>% 
  ungroup()

by_party_cycle_senate

func_plot_party_cycle(by_party_cycle_senate)


#just HOUSE candidates
by_party_cycle_house <- matchresults_joined %>% 
  filter(cand_office == "H") %>% 
  group_by(cycle, cmte_pty_affiliation) %>% 
  summarise(totdollars = sum(transaction_amt)) %>% 
  ungroup()

by_party_cycle_house

func_plot_party_cycle(by_party_cycle_house)

















