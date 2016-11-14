-------------------------
Yelp Data Set Challenge
-------------------------

<h3>Measuring the impact of a users ego network on itself and its overall impact on a business</h3>


This repository contains the code to our solution to the yelp data set challenge for 2015. We have two components in our solution which are 


1. Personalized Page Rank Component
2. Feature Formation Component
3. Data Analysis and Building Statistical Model Component


The first component is used for calculating personalized page rank values for yelp users which we use as a feature for data analysis. We leveraged the power of the open source implementation of map reduce that is Hadoop for calculating user pageranks.

The second component are a set of standalone java files that we use for a variety of tasks which are cleaning our data, studying our data by performing preliminary analysis, and also for feature formation. We use MongoDB as the backend non relational store where our data sits. 

The third component is building statistical models on our final dataset and performing some data analysis to get some cool findings. We make use of the available R platform to build our models.

More detailed information about each of the component could be found in the individual md files of each component.