----------------------------------------------
Personalized Page Rank Component Description
----------------------------------------------


This component calculates the personalized page rank value for a given node/vertex/user in the graph. This implementation does not consider the random jump factor as there are no dangling nodes in the graph.

<h3>Implementation Details</h3>

The programs are Hadoop Map Reduce programs (Java) which contain the following parts. 

1. Build personalized page rank records

Here we set the initial page rank values of all the sources to appropriate values for the further iterations. The Source nodes have a value of 1 and the rest are initialized to 0.

2. Partition Graph

The graph here is partitioned using a range partitioner so that we can split the computation on different nodes to make it faster.

3. Compute Personalized Page Rank

This program calculates the page rank value for the sources passed. The sources must match those passed in step 1.

<h3>Commands for the above steps are as follows:</h3>

<h4>Building personalized page rank records</h4>

hadoop jar target/INFM750-0.0.1-SNAPSHOT-fatjar.jar edu.umd.nkher.BuildPersonalizedPageRankRecords \
	-input YelpGraph.txt -output PageRankRecords -numNodes 174094 -sources fHtTaujcyKvXglE33Z5yIw
	
<h4>Partition Graph</h4>

hadoop jar target/INFM750-0.0.1-SNAPSHOT-fatjar.jar edu.umd.nkher.PartitionGraph \
	-input PageRankRecords -output PageRank/iter0000 -numPartitions 5 -numNodes 174094

<h4>Run Personalized Page Rank Basic</h4>

hadoop jar target/INFM750-0.0.1-SNAPSHOT-fatjar.jar edu.umd.nkher.RunPersonalizedPageRankBasic \
	-base PageRank -numNodes 174094 -start 0 -end 20 -sources fHtTaujcyKvXglE33Z5yIw 


----------------------------------------------
Acknowledgements & References
----------------------------------------------

This implementation for personalized page rank is completed with the help of the normal page rank implementation by Jimmy Lin (Professor and Dean of Research at University of Maryland) and Michael Schatz which can be found <a href="https://github.com/lintool/Cloud9/tree/master/src/main/java/edu/umd/cloud9/example/pagerank">here</a>. 



