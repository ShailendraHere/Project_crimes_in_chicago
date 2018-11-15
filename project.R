p<-proc.time()
Sys.setenv(HADOOP_HOME="/home/student/hadoop-2.7.0")
Sys.setenv(HADOOP_CMD="/home/student/hadoop-2.7.0/bin/hadoop")
Sys.setenv(JAVA_HOME='/usr/local/jdk1.8.0_151')
library(rmr2)
library(rJava)
library(rhdfs)
hdfs.init()

aa<-hdfs.read.text.file('/project/data1')
bb<-hdfs.read.text.file('/project/data2')
cc<-hdfs.read.text.file('/project/data3')
dd<-hdfs.read.text.file('/project/data4')
f<-function(x)
{
  is<-strsplit(x,split=",")
  #length(is)
  #ishead<-is[1:100]
  
  l<-list()
  for(i in 2:length(is))
  {
    a<-unlist(is[i])
    if(length(a)==24)
    {
      l[i-1] <- is[i]
    }      
  }
  ll<-l[!sapply(l,is.null)]
}
a_list<-f(aa)
b_list<-f(bb)
c_list<-f(cc)
d_list<-f(dd)
fuc<-function(x)
{
  a1<-data.frame(matrix(unlist(a_list),ncol=24,byrow= T), stringsAsFactors =FALSE)
}
a<-fuc(a_list)
b<-fuc(b_list)
c<-fuc(c_list)
d<-fuc(d_list)
feature_selection<-function(x){
  x<-x[,c(-1,-23,-24)]
}
a<-feature_selection(a)
b<-feature_selection(b)
c<-feature_selection(c)
d<-feature_selection(d)

e<-rbind(a,b,c,d)
headers<-c("ID","Case Number","Date","Block","IUCR","Primary Type","Description",
           "Location Description","Arrest","Domestic","Beat","District","Ward",
           "Community Area","FBI Code","X Coordinate","Y Coordinate","Year","Updated On",
           "Latitude","Longitude")

colnames(a)<-headers
colnames(b)<-headers
colnames(c)<-headers
colnames(d)<-headers


e<-rbind(a,b,c,d)

e_copy<-as.data.frame(apply(e[,c(1,5,11:18,20,21)],2,FUN = function(x) as.numeric(x)))

e_copy_string<-data.frame(apply(e[,c(2,4,6,7,8)],2,FUN = function(x) as.character(x)),stringsAsFactors = FALSE)

#e_copy_date<-as.data.frame(apply(e[,c(3,19)],2,FUN = function(x)   as.POSIXct(as.numeric(as.character(x)),origin="1970-01-01")))

library(lubridate)  
x1<-data.frame(parse_date_time(e$Date,orders =  'mdY HMS'),stringsAsFactors = FALSE)
x2<-data.frame(parse_date_time(e$`Updated On`,"mdY HMS"),stringsAsFactors = FALSE)
e_copy_with_date<-cbind(e_copy,x1,x2,e_copy_string)
str(e_copy_with_date)
e_copy_with_date$Arrest<-e$Arrest
e_copy_with_date$Domestic<-e$Domestic
#e_again<-data.frame(cbind(e_copy,e_copy_date,e_copy_string),stringsAsFactors = FALSE)
final_e<-e_copy_with_date
colnames(final_e)<-headers
dim(final_e)
final_e<-final_e[!is.na(final_e$Date),]#removing 2577 records not a big deal though
dim(final_e)
#rm(list = c('e','e_again','e_copy','e_copy_date','e_copy_string','e_copy_with_date','x1','x2'))
#rm(list = c('h','headers','p','updated_on','xx'))

#headers<-c("ID","IUCR","Beat","District","Ward",
#           "Community Area","FBI Code","X Coordinate","Y Coordinate","Year","Latitude","Longitude","Case Number","Block","Primary Type","Description",
#           "Location Description",
#           "Date","Updated On","Arrest","Domestic")

#colnames(final_e)<-headers
library(dplyr)
library(ggplot2)
library(mice)
final_e%>%count((`Primary Type`))%>%arrange(desc(n))
year_crime<-final_e%>%group_by(Year,Description)%>%summarise(count=n())%>%arrange(desc(count))%>%arrange(Year)
proc.time()-p

#glm(final_e$IUCR ~ final_e$`Primary Type`+final_e$Description) not a viable option
#iucr<-read.csv("/home/student/IUCR.csv",sep = ",") amey said so
final<-final_e[-which(is.na(final_e$IUCR)),]
final<-final[-which(is.na(final$District)),]
final<-final[-which(is.na(final$`Community Area`)),]
final<-final[-which(is.na(final$Ward)),]

final%>%filter(final$ID %in% unique(final$ID))

year_crime<-final%>%group_by(Year,Description)%>%summarise(count=n())%>%arrange(desc(count))%>%arrange(Year)
rm(final_e)
final_cp<-final
final$Arrest<-droplevels(final$Arrest)
final$Domestic<-droplevels(final$Domestic)  
final%>%group_by(Arrest,Description)%>%summarise(n=n())%>%arrange(desc(n))
#factor levels for arrest are "True"
final%>%group_by(Arrest)%>%summarise(n=n())#arrest==true  is 1859961 else 1859961

arrest_wise<-final%>%select(IUCR,Description,Beat,Arrest)%>%filter(Arrest=="True")%>%group_by(Description)%>%summarise(n=n())%>%arrange(desc(n))

#district wise
district_wise<-final%>%group_by(District,`Community Area`)%>%summarise(count=n())%>%arrange(desc(count)) 
#beat wise
beat_wise<-final%>%group_by(Beat)%>%summarise(count=n())%>%arrange(desc(Beat))



# feature engineering
library(lubridate)
final$weekday<-weekdays(final$Date)
final$quarter<-quarter(final$Date)
final$hour<-as.data.frame(as.character(final$Date))
final$hour<-substr(qq$`as.character(final$Date)`,12,16)

final%>%filter(quarter %in% c(1,2))%>%group_by(`Primary Type`,weekday)%>%summarise(count=n())%>%arrange(desc(count))
final%>%filter(Arrest=="True")%>%group_by(weekday,`Primary Type`)%>%summarise(count=n())%>%arrange(desc(count))
hourly<-final%>%group_by(hour)%>%summarise(n=n())%>%arrange(desc(n))
