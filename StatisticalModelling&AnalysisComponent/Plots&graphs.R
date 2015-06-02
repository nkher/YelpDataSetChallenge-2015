# PLOTS 

#set the working directory
setwd("/Users/sarika1/Desktop/yelp")

# load business_user and review CSV files in R 
review <- read.csv("review.csv",stringsAsFactors = TRUE)
business <- read.csv("business.csv", stringsAsFactors = TRUE)
business_review <- read.csv("Buss-Rev_FD.csv", stringsAsFactors =TRUE)
plot(business$business_stars,business$business_review_count)
abline(lm(business$business_stars ~ business$business_review_count))

# Plot colored histograms
hist(business_review$Review.Count, freq = 300 , breaks= 30 , col= colors)
colors = c("red", "yellow", "green", "violet", "orange", "blue", "pink", "cyan") 
setwd("/Users/sarika1/Desktop/yelp/Merge_dataset/")


# Plot all the businesses using their location latitude and longitude 

Yelp_dataset <- read.csv("Yelp_Final_Dataset.csv", stringsAsFactors = TRUE)
View(business)
View(Yelp_dataset)
str(Yelp_dataset)

install.packages('sp')
install.packages("rworldmap")
library("rworldmap")
newmap <- getMap(resolution = "low")
plot(newmap , xlim = c(-20,59),ylim = c(35,71), asp =1 , col="green")
points(business$longitude , business$latitude , col= "red")
newmap


install.packages('ggmap')
library(ggmap)
map <- get_map(location = 'USA' , zoom = 4)
mapPoints <- ggmap(map) + geom_point( data=business , aes(x= business$longitude , y = business$latitude) ,alpha = .5 , col= "red")
mapPoints
