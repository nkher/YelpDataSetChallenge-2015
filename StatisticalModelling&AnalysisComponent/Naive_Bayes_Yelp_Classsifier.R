# Used Naive bayes to build classifier which given a User dataset with variables like closeness with their ego network,
# social sentiment of their friends and influence score and price range of the business will be classifying if the users
# review the business or not 

setwd("/Users/sarika1/Desktop/yelp/classification/")

# Packages Required
install.packages("aod")
install.packages("e1071")
install.packages("gmodels")
library("gmodels")
library("e1071")
library("aod")
library("ggplot2")


# Load the dataset in the dataframe 
Yelp_dataset <- read.csv("classification_dataset.csv", stringsAsFactors = TRUE)


# Transform the variables in to factors 
# Dependent Variavle : ReviewedOrNot 
Yelp_dataset <- transform(Yelp_dataset , ReviewedOrNot = 
                            ifelse(Yelp_dataset$Reviewed == 0, "No Review given", "Reviewed"))


View(Yelp_dataset)
write.csv(Yelp_dataset, file="test.csv")

Yelp_dataset$attributes.Price.Range <- as.factor(Yelp_dataset$attributes.Price.Range)
Yelp_dataset <- transform(Yelp_dataset, Influence =
                            ifelse(Yelp_dataset$Social.Influence <= 0 , "My friends dont influence me",
                                   ifelse(Yelp_dataset$Social.Influence > 0.5 ,"Very Influential" , "Sometimes")                              
                            )                         
)



Yelp_dataset <- transform(Yelp_dataset, Closeness =
                            ifelse(Yelp_dataset$Personalized.Page.Rank <= 0.20 , "less closeness",
                                   ifelse(Yelp_dataset$Personalized.Page.Rank < 0.25 ,"Close " ,
                                          ifelse(Yelp_dataset$Personalized.Page.Rank >= 0.25, "Very Close", "NA")
                                   )
                             )                         
)


Yelp_dataset <- transform(Yelp_dataset, What_your_friends_think  = 
                            ifelse (Yelp_dataset$Social.Sentiment.Score == 0, "Nah din't review",
                                    ifelse (Yelp_dataset$Social.Sentiment.Score < 0, "Not so cool", " You can try this one")
                            )
)


View(Yelp_dataset)

Yelp_dataset$Influence <- as.factor(Yelp_dataset$Influence)
Yelp_dataset$Closeness <- as.factor(Yelp_dataset$Closeness)
Yelp_dataset$What_your_friends_think <- as.factor(Yelp_dataset$What_your_friends_think)

Yelp_dataset <- transform(Yelp_dataset, User_Review = 
                            ifelse (Yelp_dataset$user_average_stars <= 1, "Very Bad",
                                    ifelse (Yelp_dataset$user_average_stars == 2, "Bad",
                                            ifelse (Yelp_dataset$user_average_stars == 3, "Ok",
                                                    ifelse (Yelp_dataset$user_average_stars == 4, "Good", "Very Good")
                                            )
                                    )
                            )
)

str(Yelp_dataset)

Yelp_dataset <- Yelp_dataset[,-1:-3]
Yelp_dataset <- Yelp_dataset[,-1:-6]
Yelp_dataset <- Yelp_dataset[,-5]
Yelp_dataset <- Yelp_dataset[,-3]




# Split the dataset in to training and test datasets
set.seed(12345)
randomized_data <- Yelp_dataset[order(runif(nrow(Yelp_dataset))),]
na.omit(randomized_data)
Yelp_dataset_traning <- randomized_data[1:111000,]
Yelp_dataset_test  <-  randomized_data[111001:158218,]
str(Yelp_dataset_traning)


# if not for entire dataset do this;
Yelp_dataset_traning$Social.Influence <- as.factor(Yelp_dataset_traning$Social.Influence)
Yelp_dataset_traning$fans <- as.factor(Yelp_dataset_traning$fans)
Yelp_dataset_traning$review_count <- as.factor(Yelp_dataset_traning$review_count)
Yelp_dataset_traning$Personalized.Page.Rank <- as.factor(Yelp_dataset_traning$Personalized.Page.Rank)
Yelp_dataset_traning$user_average_stars <- as.factor(Yelp_dataset_traning$user_average_stars)
Yelp_dataset_traning$user_review_count <- as.factor(Yelp_dataset_traning$user_review_count) 


Yelp_dataset_test$Social.Influence <- as.factor(Yelp_dataset_test$Social.Influence)
Yelp_dataset_test$fans <- as.factor(Yelp_dataset_test$fans)
Yelp_dataset_test$review_count <- as.factor(Yelp_dataset_test$review_count)
Yelp_dataset_test$Personalized.Page.Rank <- as.factor(Yelp_dataset_test$Personalized.Page.Rank)
Yelp_dataset_test$user_average_stars <- as.factor(Yelp_dataset_test$user_average_stars)
Yelp_dataset_test$user_review_count <- as.factor(Yelp_dataset_test$user_review_count) 


# Train the model for Naive Bayes Classifier 
Yelp_Naive_Bayes <- naiveBayes(Yelp_dataset_traning, Yelp_dataset_traning$ReviewedOrNot)
# model
Yelp_Naive_Bayes
# Predict using the trained model and test dataset 
Yelp_Naive_Bayes_Pred <- predict(Yelp_Naive_Bayes, Yelp_dataset_test)

summary(Yelp_Naive_Bayes_Pred)
# Cross verify the classifier results using the Confusion Matrix
CrossTable(Yelp_Naive_Bayes_Pred, Yelp_dataset_test$ReviewedOrNot,prop.chisq = FALSE, prop.t = FALSE, dnn = c('predicted','actual'))


col1 <- as.numeric(Yelp_dataset_test$ReviewedOrNot)
col2 <- as.numeric(Yelp_dataset_test$ReviewedOrNot)
col1
col2
cor(col1, col2)

# Classifier Results 
# 
# Cell Contents
# |-------------------------|
#   |                       N |
#   |           N / Row Total |
#   |           N / Col Total |
#   |-------------------------|
#   
#   
#   Total Observations in Table:  47218 
# 
# 
# | actual 
# predicted | Not Reveiw given |         Reviewed |        Row Total | 
#   -----------------|------------------|------------------|------------------|
#   Not Reveiw given |            23638 |                0 |            23638 | 
#   |            1.000 |            0.000 |            0.501 | 
#   |            1.000 |            0.000 |                  | 
#   -----------------|------------------|------------------|------------------|
#   Reviewed |                0 |            23580 |            23580 | 
#   |            0.000 |            1.000 |            0.499 | 
#   |            0.000 |            1.000 |                  | 
#   -----------------|------------------|------------------|------------------|
#   Column Total |            23638 |            23580 |            47218 | 
#   |            0.501 |            0.499 |                  | 
#   -----------------|------------------|------------------|------------------|
