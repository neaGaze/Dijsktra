1.	goto /usr/local/hadoop/worksapce/bfs
2.	Use "hadoop dfs -rmr db3_output--1" to delete output directory
3.	Use "$HADOOP_COMMON_HOME/bin/hadoop jar ./bfs.jar db3_input db3_output" to compile
4.	Use "$HADOOP_HOME/bin/hadoop fs -put /home/neagaze/workspace/Dijkstra/input/test.txt db3_input" to copy input from local machine
