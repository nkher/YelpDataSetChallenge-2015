# Decision Trees and Rules for classification problem 
# Research Question : Given a sentiment score for a business,personalized pagerank  (Closness)  and social influence measure 
# can predict if a user has given a review for the business or not

setwd("/Users/sarika1/Desktop/yelp/classification/")

# Packages Required
install.packages("tree")
install.packages("rpart.plot")
install.packages("caret")
install.packages("lattice")
install.packages("rpart")
library("tree")
require("tree")
library("rpart")
require("rpart")
library("rpart.plot")
library("RColorBrewer")
library("caret")


# Load the dataset in the dataframe 
Yelp_dataset <- read.csv("classification_dataset.csv", stringsAsFactors = TRUE)


# Transform the variables in to factors 
# Dependent Variavle : ReviewedOrNot 
Yelp_dataset <- transform(Yelp_dataset , ReviewedOrNot = 
                            ifelse(Yelp_dataset$Reviewed == 0, "I dint review ", "I reviewed"))


Yelp_dataset$attributes.Price.Range <- as.factor(Yelp_dataset$attributes.Price.Range)
Yelp_dataset <- transform(Yelp_dataset, Influence =
                            ifelse(Yelp_dataset$Social.Influence <= 0 , "My friends dont influence me",
                                   ifelse(Yelp_dataset$Social.Influence > 1.5 ,"Very Influential" , "Sometimes")                              
                            )                         
)



Yelp_dataset <- transform(Yelp_dataset, Closeness =
                            ifelse(Yelp_dataset$Personalized.Page.Rank <= 0 , "no closeness",
                                   ifelse(Yelp_dataset$Personalized.Page.Rank > 0.15 ,"Very close " , "Close")                              
                            )                         
)


Yelp_dataset <- transform(Yelp_dataset, What_your_friends_think  = 
                            ifelse (Yelp_dataset$Social.Sentiment.Score == 0, "You can try this one",
                                    ifelse (Yelp_dataset$Social.Sentiment.Score < 0, "Not so cool", " Must go")
                            )
)




Yelp_dataset$Influence <- as.factor(Yelp_dataset$Influence)
Yelp_dataset$Closeness <- as.factor(Yelp_dataset$Closeness)

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

Yelp_dataset <- Yelp_dataset[,-6]

# Split the dataset in to training and test datasets
set.seed(12345)
randomized_data <- Yelp_dataset[order(runif(nrow(Yelp_dataset))),]
na.omit(randomized_data)
Yelp_dataset_traning <- randomized_data[1:79109,]
Yelp_dataset_test  <-  randomized_data[79110:158218,]
str(Yelp_dataset_traning)

decision_tree <- tree(Yelp_dataset_traning$ReviewedOrNot~ ., data= Yelp_dataset_traning)
plot(decision_tree)
text(decision_tree, pretty =0)


# check how the model doing using the test data 

decision_tree_pred <- predict(decision_tree , data= Yelp_dataset_test, type= "class")
decision_tree
cv_tree <- cv.tree(decision_tree, FUN = prune.misclass)

names(cv_tree)
plot(cv_tree$size,cv_tree$dev , type= "b")

# prune the tree 
pruned_tree_model <- prune.misclass(decision_tree, best =3)
plot(pruned_tree_model)
text(pruned_tree_model, pretty =0)

pruned_tree_pred <- predict(pruned_tree_model, data = Yelp_dataset_test)

######################################### Rules Algorithms ###################################################################
# OneR  rule Method
-------------------------------
  
install.packages("RWeka")
library("RWeka")
require("RWeka")
OneR_model <- OneR(Yelp_dataset_traning$ReviewedOrNot~ Yelp_dataset_traning$attributes.Price.Range + Yelp_dataset_traning$Influence
                   + Yelp_dataset_traning$Closeness + Yelp_dataset_traning$What_your_friends_think , data= Yelp_dataset_traning)
OneR_model
summary(OneR_model)
# OneR_model  
# Results: 
# Yelp_dataset_traning$What_your_friends_think:
# You can try this one	-> Reviewed
# Nah dint review	-> Not Reveiw given
# Not so cool	-> Reviewed
# (76291/111000 instances correct)

# Interpretation 
# This shows that the What_your_friends_think( User friends sentiment about the business)
# is useful to decide if user has reviewed or not. This classifier classifies 76291 instances correctly 
OneR_predict <- predict(OneR_model,data=Yelp_dataset_test)

str(Yelp_dataset_test)
summary(OneR_predict)
nrow(Yelp_dataset_test)
nrow(Yelp_dataset_traning)
CrossTable(OneR_predict, Yelp_dataset_traning$ReviewedOrNot,prop.chisq = FALSE, prop.t = FALSE, dnn = c('predicted','actual'))

View(Yelp_dataset_test)


#JRipper Algorithm 

JRip_classfier <- JRip(Yelp_dataset_traning$ReviewedOrNot~ ., data= Yelp_dataset_traning)
JRip_classfier
JRipclassifier_predict <- predict(JRip_classfier,data= Yelp_dataset_test$ReviewedOrNot)

JRipclassifier_predict
CrossTable(OneR_predict, Yelp_dataset_test$ReviewedOrNot,prop.chisq = FALSE, prop.t = FALSE, dnn = c('predicted','actual'))

str(Yelp_dataset_traning)
str(Yelp_dataset_test)
