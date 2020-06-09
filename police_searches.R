library(RODBC)
library(dplyr)
library(ikit)
library(tidyverse)
library(lubridate)
library(janitor)
library(dbplyr)
library(readxl)
library(writexl)
library(fst)
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

#how many entity types?
contribs_db %>% 
  count(entity_tp)


#uppercase the target fields
contribs_db <- contribs_db %>% 
  mutate(
    occupation = str_trim(str_to_upper(occupation)),
    employer = str_trim(str_to_upper(employer))
  )


#create sql statement to feed match terms to the database
sql <- "SELECT *
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
matchresults_all <- dbGetQuery(con, sql)

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
matchresults_lookuptable <- matchresults_all %>% 
  select(uniqueidstring, employer, occupation) %>% 
  distinct()

head(matchresults_lookuptable)

#save to file
write_csv(matchresults_lookuptable, "processed_data/matchresults_lookuptable_orig.csv")





