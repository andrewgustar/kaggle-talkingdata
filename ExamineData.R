library(dplyr)
library(tidyr)

gender_age_train <- read.csv("~/R Statistics/Kaggle/TalkingDataMobile/data/gender_age_train.csv",numerals="no.loss",stringsAsFactors = FALSE) #74645 obs of 4 variables device, gender, age, group

gender_age_test <- read.csv("~/R Statistics/Kaggle/TalkingDataMobile/data/gender_age_test.csv",numerals="no.loss",stringsAsFactors = FALSE) #112071 obs of 1 variable device

full_data <- rbind(gender_age_train[,c(1,4)],data.frame(gender_age_test,group=NA))
train <- c(rep(TRUE,nrow(gender_age_train)),rep(FALSE,nrow(gender_age_test)))

rm(gender_age_test,gender_age_train)

#sample_submission <- read.csv("~/R Statistics/Kaggle/TalkingDataMobile/data/sample_submission.csv",numerals="no.loss",check.names = FALSE) #device and groups - first col is device as in gender_age_test
#====================================

phone_brand_device_model <- read.csv("~/R Statistics/Kaggle/TalkingDataMobile/data/phone_brand_device_model.csv", encoding="UTF-8",numerals="no.loss",stringsAsFactors = FALSE) #187245 obs of 3 variables - device brand model
phone_brand_device_model <- distinct(phone_brand_device_model,device_id) %>%
  mutate(brand_model=paste(phone_brand,device_model))
phone_brands <- as.data.frame(table(phone_brand_device_model$phone_brand),stringsAsFactors = FALSE) #131 brands
phone_brands <- arrange(phone_brands,desc(Freq)) %>% 
  mutate(replace=ifelse(Freq<1000,"Other",Var1))

phone_models <- as.data.frame(table(phone_brand_device_model$brand_model),stringsAsFactors = FALSE) #1667 models
phone_models <- arrange(phone_models,desc(Freq)) %>% 
  mutate(replace=ifelse(Freq<500,"Other",Var1)) 

length(unique(phone_brands$replace)) #reduced to 15
length(unique(phone_models$replace)) #reduced to 74

replace_brands <- unique(filter(phone_brands,Freq<1000) %>% select(Var1))
replace_models <- unique(filter(phone_models,Freq<500) %>% select(Var1))

phone_brand_device_model <- mutate(phone_brand_device_model,top_brand=ifelse(phone_brand %in% replace_brands$Var1,"Other",phone_brand))
phone_brand_device_model <- mutate(phone_brand_device_model,top_model=ifelse(brand_model %in% replace_models$Var1,"Other",brand_model))

full_data <- left_join(full_data,phone_brand_device_model[,c(1,5,6)],by="device_id")

rm(phone_brand_device_model,phone_models,phone_brands,replace_models,replace_brands)
#================================================

events <- read.csv("~/R Statistics/Kaggle/TalkingDataMobile/data/events.csv",numerals="no.loss",stringsAsFactors = FALSE) #3252950 obs of 5 variables - event, device, timestamp, lat, long - time of day, weekday might be relevant

#set up time based indicators
events <- distinct(events) 
period <- as.POSIXlt(events$timestamp)
period$hour <- 3*floor(period$hour/3)
period$min <- 0
period$sec <- 0
events$period <- as.character(period)

ev_av_by_device <- group_by(events,device_id,period) %>%
  summarise(count=n()) %>% 
  mutate(period=as.POSIXct(period),
         timeslot=format(period,format="%H"),
         weekday=format(period,format="%A"),
         weektime=paste(weekday,timeslot)) %>% 
  group_by(device_id,weektime) %>%
  summarise(count=mean(count)) %>%
  spread(weektime,count,fill=0)
full_data <- left_join(full_data,ev_av_by_device,by="device_id")
rm(ev_av_by_device,period)

#analyse lat and long - min and max (or range) and average
location_data <- filter(events,longitude!=0) %>%
  group_by(device_id) %>%
  summarise(long_av=mean(longitude),
            lat_av=mean(latitude),
            long_range=max(longitude)-min(longitude),
            lat_range=max(latitude)-min(latitude))
full_data <- left_join(full_data,location_data,by="device_id")
rm(location_data)
save(full_data,train,file="full_data.RData")

#===============================================
#cut up data into manageable chunks
library(data.table)

app_events <- fread("~/R Statistics/Kaggle/TalkingDataMobile/data/app_events.csv",colClasses = c("integer","character","integer","integer"))
app_events_active <- app_events[is_active==1,.(event_id,app_id)]
app_events_installed <- app_events[,.(event_id,app_id)]
app_events_installed_1 <- app_events_installed[event_id<1625564]
app_events_installed_2 <- app_events_installed[event_id>=1625564]

write.csv(app_events_active,"~/R Statistics/Kaggle/TalkingDataMobile/data/app_events_active.csv",row.names = FALSE)
write.csv(app_events_installed_1,"~/R Statistics/Kaggle/TalkingDataMobile/data/app_events_installed_1.csv",row.names = FALSE)
write.csv(app_events_installed_2,"~/R Statistics/Kaggle/TalkingDataMobile/data/app_events_installed_2.csv",row.names = FALSE)
rm(list=ls())

load("full_data.RData")
events <- ungroup(events) %>% select(event_id,device_id,period)
write.csv(events,"~/R Statistics/Kaggle/TalkingDataMobile/data/events_xref.csv",row.names = FALSE)
rm(list=ls())

app_events_active <- fread("~/R Statistics/Kaggle/TalkingDataMobile/data/app_events_active.csv",colClasses="character") #32473067 obs of event, app, is installed, is active
#need to reduce this to info per device - first reduce to app categories and deduplicate, then perhaps look at number of categories, number of events in a period, type/location/time of last three events - also reduce to device_id and timeslot

app_labels <- fread("~/R Statistics/Kaggle/TalkingDataMobile/data/app_labels.csv",colClasses="character") #459943 obs of 2 variables - appID, app category label
app_labels <- distinct(app_labels,app_id)

label_categories <- fread("~/R Statistics/Kaggle/TalkingDataMobile/data/label_categories.csv",colClasses="character") #930 obs of 2 variables - app category label, description

events_xref <- fread("~/R Statistics/Kaggle/TalkingDataMobile/data/events_xref.csv",colClasses="character")

app_events_active <- merge(app_events_active,events_xref,by="event_id")
app_events_active <- merge(app_events_active,app_labels,by="app_id",all.x=TRUE)
save(app_events_active,file="app_events_active.RData")

app_events_active <- select(app_events_active,device_id,label_id,period) %>%
  group_by(device_id,period,label_id) %>%
  summarise(count=n())
save(app_events_active,file="app_events_active.RData")

rm(events_xref,app_labels,label_categories)

app_events_active <- mutate(app_events_active,period2=as.POSIXct(period),
       timeslot=format(period2,format="%H"),
       weekday=format(period2,format="%A"),
       weektime=paste(weekday,timeslot)) %>% 
  group_by(device_id,label_id,weektime) %>%
  summarise(count=mean(count))
  
save(app_events_active,file="app_events_active.RData")
#60669 devices, 328 app labels

events_xref <- fread("~/R Statistics/Kaggle/TalkingDataMobile/data/events_xref.csv",colClasses="character")
app_labels <- fread("~/R Statistics/Kaggle/TalkingDataMobile/data/app_labels.csv",colClasses="character") #459943 obs of 2 variables - appID, app category label
app_labels <- distinct(app_labels,app_id)
app_events_installed_1 <- fread("~/R Statistics/Kaggle/TalkingDataMobile/data/app_events_installed_1.csv",colClasses="character")
app_events_installed_1 <- merge(app_events_installed_1,events_xref,by="event_id")
app_events_installed_1 <- merge(app_events_installed_1,app_labels,by="app_id",all.x=TRUE)
app_events_installed_1 <- select(app_events_installed_1,device_id,label_id) %>%
  group_by(device_id) %>%
  summarise(count=length(unique(label_id)))

app_events_installed_2 <- fread("~/R Statistics/Kaggle/TalkingDataMobile/data/app_events_installed_2.csv",colClasses="character")
app_events_installed_2 <- merge(app_events_installed_2,events_xref,by="event_id")
app_events_installed_2 <- merge(app_events_installed_2,app_labels,by="app_id",all.x=TRUE)
app_events_installed_2 <- select(app_events_installed_2,device_id,label_id) %>%
  group_by(device_id) %>%
  summarise(count=length(unique(label_id)))
#doesn't work - can't combine the two! - or perhaps by taking max

load("full_data.RData")