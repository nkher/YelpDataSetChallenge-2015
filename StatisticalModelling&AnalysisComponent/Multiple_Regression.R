setwd("/Users/sarika1/Desktop/yelp/regression/")

#insall the required packages
install.packages("psych")
library('glmnet')
library('psych')

# Load the dataset in to a dataframe 
Yelp_dataset <- read.csv("regression_data.csv", stringsAsFactors= TRUE)
# Total number of values : 158218
nrow(Yelp_dataset)


# Split the dataset in to training and test datasets
set.seed(12345)
randomized_data <- Yelp_dataset[order(runif(nrow(Yelp_dataset))),]
na.omit(randomized_data)
Yelp_dataset_traning <- randomized_data[1:79109,]
Yelp_dataset_test  <-  randomized_data[79110:158218,]

# Step Regression to get the best equation with variables that affect the business most
multiple_regression_model <- lm(Yelp_dataset_traning$stars ~ ., data= Yelp_dataset_traning)
multiple_regression_model2 <- lm(Yelp_dataset_traning$stars ~ Yelp_dataset_traning$Social.Sentiment.Score+ Yelp_dataset_traning$Social.Influence
                                + Yelp_dataset_traning$Personalized.Page.Rank + Yelp_dataset_traning$fans)

multiple_regression_model3 <- lm(Yelp_dataset_traning$stars ~ Yelp_dataset_traning$Personalized.Page.Rank + Yelp_dataset_traning$fans + Yelp_dataset_traning$user_average_stars
                                 + Yelp_dataset_traning$attributes.Price.Range + Yelp_dataset_traning$user_review_count)
formula = step(multiple_regression_model , direction = "backward")
formula(formula)
summary(multiple_regression_model)

cor(Yelp_dataset_traning[c("stars","Social.Influence","Personalized.Page.Rank", "Social.Sentiment.Score","user_average_stars")])
cor(Yelp_dataset_traning[c("stars","Personalized.Page.Rank","attributes.Price.Range")])
str(Yelp_dataset_traning)
pairs.panels(Yelp_dataset_traning[c("stars","Social.Influence","Personalized.Page.Rank", "Social.Sentiment.Score")])



nrow(Yelp_dataset_test)

business_model_pedicted <- predict(multiple_regression_model,  Yelp_dataset_test)
cor(business_model_pedicted,Yelp_dataset_test$stars)

cor(business_model_pedicted, Yelp_dataset_test$stars)

write.csv(business_model_pedicted, "predict.csv")
summary(business_model_pedicted)      
nrow(business_model_pedicted)
View(business_model_pedicted)
cor(Yelp_dataset_traning$Social.Sentiment.Score, Yelp_dataset_traning$Social.Influence, method = "pearson")
cor(business_model_pedicted, na.omit(Yelp_dataset_traning$stars))
cor(Yelp_dataset_traning[c("stars","Personalized.Page.Rank", "attributes.Price.Range","user_average_stars","fans","review_count")])

pairs.panels(Yelp_dataset_traning[c("stars","Personalized.Page.Rank", "attributes.Price.Range","user_average_stars","fans","review_count")])
# Decision Tree for Regression
setwd("/Users/sarika1/Desktop/yelp/")
Yelp_dataset_Full <- read.csv("Yelp_Final_Dataset.csv" , stringsAsFactors = TRUE)
View(Yelp_dataset_traning)
install.packages('tree')
library(tree)
require(tree)
set.seed(12345)
randomized_data <- Yelp_dataset_Full[order(runif(nrow(Yelp_dataset))),]
na.omit(randomized_data)
Yelp_dataset_traning <- randomized_data[1:79109,]
Yelp_dataset_test  <-  randomized_data[79110:158218,]

Yelp_decision_tree <- tree(Yelp_dataset_traning$stars ~ ., data = Yelp_dataset_traning)

Yelp_decision_tree
