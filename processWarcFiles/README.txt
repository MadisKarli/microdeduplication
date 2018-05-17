Java program for extracting ntriples (n-quads) from Warc files. Forked from https://github.com/KhalilRehman/microdeduplication 

Execution in cluster: 
	spark-submit --master yarn processSeqFiles-0.1-jar-with-dependencies.jar <inputfilepath> <outputfilepath>
	
	<inputfilepath> : hdfs path of the input directory. 
	<outfilepath> : hdfs path to the output directory. The output will be standard Spark output directory, that has a structure of a csv file.

Example:
Let's have warc files at "/crawl1/warcs". Then we will use the command:
	spark-submit --master yarn processSeqFiles-0.1-jar-with-dependencies.jar /crawl1/warcs crawl1_ntriples

This will create "crawl1_ntriples" in hdfs directory "/user/madis/" (as no path is specified).


Execution in local:
	java -jar target/deduplicaiton-0.0.1-SNAPSHOT-jar-with-dependencies.jar <inputfilepath> <outputfilepath>
	
	<inputfilepath> : filesystem path of the input directory.
	<outfilepath> : filesystem path to the output directory. The output will be standard Spark output directory, that is essentialy a csv file. Simply changing the ending converts it to csv.

	
Creating jar file
	Run "mvn package"

