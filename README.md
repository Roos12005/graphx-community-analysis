# graphx-community-analysis

This repository is for research on Community analysis using Spark GraphX API, It contained several modules from several contributors as below. Please give a credit to them. The purpose of this project is for integrating graph algorithms into one module and can be used as general purpose framework for graph analytic.

1. PageRank Algorithm
From Spark GraphX API
2. Closeness Centrality Algorithm
From kbastani/spark-neo4j
3. K-Betweenness Centrality Algorithm 
From dmarcous/spark-betweenness
4. Louvain Community Detection Algorithm
From Sotera/spark-distributed-louvain-modularity


# How to build
	
You have to build project with 

	build/mvn -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.0 -DskipTests clean package


# How to use
	
You can use startprocessingLocal.sh to submit program to Stand alone Spark (Local mode) or startprocessingCluster.sh to submit program to spark cluster (Yarn-client mode).


# How to specify parameter

Example on startprocessingLocal.sh

	spark-submit \
        --class org.mazerunner.extension.SparkApplication \
        --master local[*] \
        --driver-memory 30g \
        spark-1.1.5-RELEASE-driver.jar \
        MODE PARTITION INPUT_EDGE ALGORITHM OUTPUT

[parameter]

	MODE = mode to process, currently use only “TEST”
	PARTITION = number to partition e.g. 2
	INPUT_EDGE = edgeList input e.g. “dataSparse/FullGraphSparse5k.txt”
	ALGORITHM = graph algorithm to process INPUT_EDGE 
		“PR” => PageRank
		“CC” => Closeness Centrality
		“KBC” => K-betweenness Centrality
		“LC” => Louvain Community Detection Algorithm
	OUTPUT = result file e.g. "result.txt" 
 

