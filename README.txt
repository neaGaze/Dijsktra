DESCRIPTION
=================================================

This is the implementation of Dijsktra's algorithm using Mapreduce powered by Java. The IDE used for development was Eclipse.

INSTRUCTION
=================================================
Import the code in Eclipse. Make sure you have Hadoop correctly installed and setup all the libraries you may have locally (I have not used ant or gradle for handling library as this is only a lightweight program probably for a college project). 

Run the program. Move the generated bfs.jar into your hadoop path. In my case: "$HADOOP_COMMON_HOME/bin/hadoop jar ./bfs.jar db3_input db3_output"

Create two folders in your hdfs named 'db3_input' and 'db3_output'.

Copy the test input from inside the input folder into 'db3_input' folder like: "$HADOOP_HOME/bin/hadoop fs -put <your_machine>/Dijkstra/input/test.txt db3_input" 

You can start testing the program now.

You can also test on your custom datasets but make sure to modify your dataset according to the format provided below.

DATASET DETAILS
=================================================
s	1 		(This means that the starting node is 1)
1	2,3_10,5_0_GRAY	(Explained at the end of this section)
2	3,4_2,1_999_WHITE
3	2,4,5_3,9,2_999_WHITE
4	5_4_999_WHITE
5	4,1_6,7_999_WHITE

Format:

<source>	<destination 1, destination 2, ..>_<weight from source to destination 1, weight from source to destination 2,..>_<shortest path from starting node to source>_<color of the node>

