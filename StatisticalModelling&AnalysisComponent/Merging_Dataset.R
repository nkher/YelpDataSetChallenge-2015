# This program is to merge the CSV files having user details with business to get get the final dataset to perform Regression and Classification on 
# Use merge function in R to join the two datasets on common id ="business_id"
# Variables:
# Independent Variable : stars (business - ratings)
# Dependent Variable   : fans , user_review_counts , user_average_stars , social_influence , Reviewed (0 - if user has given a review / 1 - if user has not given a review),
#                        Personalized_page_rank ,  Price.range, ambience
# Total number of rows : 158218 

#set the working directory
setwd("/Users/sarika1/Desktop/yelp/Merge_dataset/")

# Load CSV into Dataframes
cd1 <- read.csv("cd1.csv", stringsAsFactors = TRUE)
cd2 <- read.csv("cd2.csv", stringsAsFactors = TRUE)
cd3 <- read.csv("cd3.csv", stringsAsFactors = TRUE)
cd4 <- read.csv("cd4.csv", stringsAsFactors = TRUE)
cd5 <- read.csv("cd5.csv", stringsAsFactors = TRUE)
cd6 <- read.csv("cd6.csv", stringsAsFactors = TRUE)
cd7 <- read.csv("cd7.csv", stringsAsFactors = TRUE)

business <- read.csv("business.csv" , stringsAsFactors= TRUE)

# Merge datasets 
business_cd1 <- merge(cd1,business, by="business_id")
business_cd2 <- merge(cd2,business, by="business_id")
business_cd3 <- merge(cd3,business, by="business_id")
business_cd4 <- merge(cd4,business, by="business_id")
business_cd5 <- merge(cd5,business, by="business_id")
business_cd6 <- merge(cd6,business, by="business_id")
business_cd7 <- merge(cd7,business, by="business_id")

# Final dataset 
final_dataset <- rbind(business_cd1,business_cd2,business_cd3,business_cd4,business_cd5,business_cd6,business_cd7)
View(final_dataset)
final_dataset <- final_dataset[,-3]
final_dataset <- final_dataset[,-12:-14]
nrow(final_dataset)

# Write to CSV 
write.csv(final_dataset,file="Yelp_Final_Dataset.csv")
