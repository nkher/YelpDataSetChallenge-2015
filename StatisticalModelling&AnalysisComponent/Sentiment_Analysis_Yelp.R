# Sentiment analysis:
# Used the "tm" - text mining package for splitting the text reviews
# Loaded the positive and negative word dictionaries in to dataframes
# Split the text reviews in to corpus of words 

#
#Sentiment Algorithm 
#     1.  Store positive words -> P {positive set}
#     2.	Store negative words -> N {negative set}
#     3.	For each review
#         a.	Set Sentiment Score  (SS = 0)
#         b.	Tokenize the text into words
#         c.	Clean words (remove punctuations, lower casing)
#             i.	For each Word ‘w’
#            ii.	If  ‘w’ in P then SS += 1
#           iii.	If  ‘w’ in N then SS += -1
#            iv.	Else Continue
#         d.	Save Sentiment Score (SS)


#set the working directory
setwd("/Users/sarika1/Desktop/yelp")

# load business_user and review CSV files in R 
review <- read.csv("review.csv",stringsAsFactors = TRUE)
View(review)


#load the necessary packages 
install.packages('RCurl')
install.packages("tm")
install.packages("ggplot2")
install.packages("SnowballC")

library(RCurl)
library("tm")
library("ggplot2")
library("SnowballC")

# Read list of positive and negative words
pos <- read.csv("positive-words.csv")
neg <- read.csv("negative-words.csv")
pos <- pos$a
pos
neg <- neg$X2.faced
neg


# split review files : 900627 values 

review1 <- review[1:100000,]
review2 <- review[100001:200000,]
review3 <- review[200001:300000,]
review4 <- review[300001:400000,]
review5 <- review[400001:500000,]
review6 <- review[500001:600000,]
review7 <- review[600001:700000,]
review8 <- review[700001:800000,]
review9 <- review[800001:900000,]
review10 <- review[900001:990635,]


#storing in a corpus 
reviews_corpus <- Corpus(VectorSource(review10$text))
reviews_corpus_clean <- tm_map(reviews_corpus, tolower, lazy=TRUE)
reviews_corpus_clean <- tm_map(reviews_corpus_clean, removeNumbers, lazy=TRUE)
reviews_corpus_clean <- tm_map(reviews_corpus_clean, removePunctuation, lazy=TRUE)
reviews_corpus_clean <- tm_map(reviews_corpus_clean, function(x) removeWords(x,stopwords()), lazy=TRUE)
inspect(reviews_corpus_clean[1:4])
reviews_corpus_clean <- lapply(reviews_corpus_clean,strsplit,"\\s+")


# to match positive and negative values 
matches <-lapply(reviews_corpus_clean,function(x){
  match.pos <- match(x[[1]],pos)
  match.neg <- match(x[[1]],neg)
  
  list(length(which(!is.na(match.pos))) , length(which(!is.na(match.neg))))
})

# creating a matrix of the number of positive and negative words
match.matrix <- matrix(unlist(matches),nrow=length(matches),ncol=2,byrow=TRUE)

# Calculating the score by subtracting positive from negative
simple.sentiment <- match.matrix[,1] - match.matrix[,2]
table(as.vector(simple.sentiment))

# plotting the matrix
colors = c("red", "yellow", "green", "violet", "orange", "blue", "pink", "cyan") 
hist(match.matrix)
hist(simple.sentiment,col=colors)

# Adding a column to a data frame for the score for each review
review_with_sentiment = data.frame(review10, simple.sentiment)
review_with_sentiment <- review_with_sentiment[,-8]
review_with_sentiment <- review_with_sentiment[,-1:-3]
View(review_with_sentiment)
nrow(review_with_sentiment)

# write to text file 
write.table(review_with_sentiment,"file10.txt" , sep="\t")

# Grep command to concat all the files 
# cat file1.txt file2.txt file3.txt file4.txt file5.txt file6.txt file7.txt file8.txt file9.txt file10.txt > sentiment_review.txt



