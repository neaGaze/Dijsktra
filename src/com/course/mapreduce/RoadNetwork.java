package com.course.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.course.mapreduce.Node.Color;

/**
 * This is an example Hadoop Map/Reduce application.
 * 
 * It inputs a map in adjacency list format, and performs a breadth-first
 * search. The input format is ID EDGES_WEIGHTS_DISTANCE|_COLOR where ID = the
 * unique identifier for a node (assumed to be an int here) EDGES = the list of
 * edges emanating from the node (e.g. 3,8,9,12) DISTANCE = the to be determined
 * distance of the node from the source COLOR = a simple status tracking field
 * to keep track of when we're finished with a node
 * 
 * It assumes that the source node (the node from which to start the search) has
 * been marked with distance 0 and color GRAY in the original input. All other
 * nodes will have input distance Integer.MAX_VALUE and color WHITE.
 */
public class RoadNetwork extends Configured implements Tool {

	public static boolean IS_ALL_BLACK = false;
	
	public static final String INPUT1_FILE_LOCATION = "db3_input/test.txt";
//	public static final String INPUT1_FILE_LOCATION = "db3_input/input2.txt";
	public static final String OUTPUT_FILE_LOCATION = "db3_output--";
	public static final String OUTPUT1_FILE_LOCATION = "db3_input/test.txt";

	// public static final Log LOG =
	// LogFactory.getLog("org.apache.hadoop.examples.GraphSearch");

	/**
	 * Nodes that are Color.WHITE or Color.BLACK are emitted, as is. For every
	 * edge of a Color.GRAY node, we emit a new Node with distance incremented
	 * by one. The Color.GRAY node is then colored black and is also emitted.
	 */
	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {

			Node node = new Node(value.toString());

			// For each GRAY node, emit each of the edges as a new node (also
			// GRAY)
			if (node.getColor() == Node.Color.GRAY) {
				int i = 0;
				for (int v : node.getEdges()) {
					Node vnode = new Node(v);
						vnode.setDistance(node.getDistance()
							+ node.getWeights().get(i));
					vnode.setColor(Node.Color.GRAY);
					i++;
					output.collect(new IntWritable(vnode.getId()), new Text(
							vnode.getLine()));
				}
				// We're done with this node now, color it BLACK
				node.setColor(Node.Color.BLACK);
			}

			// No matter what, we emit the input node
			// If the node came into this method GRAY, it will be output as
			// BLACK
			output.collect(new IntWritable(node.getId()),
					new Text(node.getLine()));

		}
	}

	/**
	 * A reducer class that just emits the shortest distance
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<IntWritable, Text, IntWritable, Text> {
		public static final Log log = LogFactory.getLog(Reduce.class);

		
		/**
		 * Make a new node which combines all information for this single node
		 * id. The new node should have - The full list of edges - The minimum
		 * distance - The darkest Color
		 */
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {

			List<Integer> edges = null;
			List<Integer> weights = null;
			int distance = Integer.MAX_VALUE;
			Node.Color color = Node.Color.WHITE;

			while (values.hasNext()) {
				Text value = values.next();
			//	System.out.println("reducer key: " + key.get() + ", values: " + value.toString() + "\n");
				Node u = new Node(key.get() + "\t" + value.toString());

				if (u.getEdges().size() > 0) {
					edges = u.getEdges();
				}

				if (u.getWeights().size() > 0) {
					weights = u.getWeights();
				}

				// Save the minimum distance
				if (u.getDistance() < distance) {
					distance = u.getDistance();
				}

				// Save the darkest color
				if (u.getColor().ordinal() > color.ordinal()) {
					color = u.getColor();
				}

				if(color != Color.BLACK)
					IS_ALL_BLACK = false;
			}
				
			Node n = new Node(key.get());
			n.setDistance(distance);
			n.setEdges(edges);
			n.setWeights(weights);
			n.setColor(color);
			output.collect(key, new Text(n.getLine()));
		}
	}

	/**
	 * For adjacency list mapper class
	 ***/
	public static class AdjacencyMap extends MapReduceBase implements
			Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {

			String[] ids = value.toString().split("\\s+");
			if(ids.length == 2)
				output.collect(new LongWritable(Long.parseLong(ids[0])), new Text(
						ids[1]));
			else
				output.collect(new LongWritable(Long.parseLong(ids[0])), new Text(
					ids[1] + " " + ids[2]));
		}
	}

	/**
	 * A reducer class that
	 */
	public static class AdjacencyReduce extends MapReduceBase implements
			Reducer<LongWritable, Text, LongWritable, Text> {

		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {

			String color = "WHITE";
			int dist = Integer.MAX_VALUE;
			
			StringBuilder edgeBuilder = new StringBuilder();
			StringBuilder weightBuilder = new StringBuilder();
			
			while (values.hasNext()) {
				Text value = values.next();
				
				// we have found the start node
				if(value.toString().equals("s")) {
					color = "GRAY";
					dist = 0;
					continue;
				}
				
				// find all the destination and weights 
				String[] id = value.toString().split("\\s+");
				if (id.length > 0) {
					edgeBuilder.append(id[0]).append(",");
					weightBuilder.append(id[1]).append(",");
				}
			}

			String edge = edgeBuilder.toString();
			String weight = weightBuilder.toString();

			if (edge != null && edge.length() > 0
					&& edge.charAt(edge.length() - 1) == ',')
				edge = edge.substring(0, edge.length() - 1);

			if (weight != null && weight.length() > 0
					&& weight.charAt(weight.length() - 1) == ',')
				weight = weight.substring(0, weight.length() - 1);

			String  valueString = edge + "_" + weight + "_" + dist + "_" + color;
		//	System.out.println("Key: " + key + ", value: " + valueString +"\n");
			output.collect(key, new Text(valueString));
		}
	}

	static int printUsage() {
		System.out
				.println("graphsearch [-m <num mappers>] [-r <num reducers>]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * For the Adjacency List configuration
	 ***/
	private JobConf getAdjacencyConf(String[] args) {
		JobConf conf = new JobConf(getConf(), RoadNetwork.class);
		conf.setJobName("graphadjacency");

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(AdjacencyMap.class);
		conf.setReducerClass(AdjacencyReduce.class);

		for (int i = 0; i < args.length; ++i) {
			if ("-m".equals(args[i])) {
				conf.setNumMapTasks(Integer.parseInt(args[++i]));
			} else if ("-r".equals(args[i])) {
				conf.setNumReduceTasks(Integer.parseInt(args[++i]));
			}
		}

		return conf;
	}
	
	/**
	 * For the Dijkstra configuration 
	 ***/
	private JobConf getJobConf(String[] args) {
		JobConf conf = new JobConf(getConf(), RoadNetwork.class);
		conf.setJobName("graphsearch");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(Reduce.class);

		for (int i = 0; i < args.length; ++i) {
			if ("-m".equals(args[i])) {
				conf.setNumMapTasks(Integer.parseInt(args[++i]));
			} else if ("-r".equals(args[i])) {
				conf.setNumReduceTasks(Integer.parseInt(args[++i]));
			}
		}

		return conf;
	}

	/**
	 * The main driver for the map/reduce program. Invoke this method to submit
	 * the map/reduce job.
	 * 
	 * @throws IOException
	 *             When there is communication problems with the job tracker.
	 */
	public int run(String[] args) throws Exception {

		// To first convert raw data into adjacency Lists
		String ip = INPUT1_FILE_LOCATION;
		String op = OUTPUT_FILE_LOCATION;
		
		JobConf initConf = getAdjacencyConf(args);
		FileInputFormat.setInputPaths(initConf, new Path(ip));
		FileOutputFormat.setOutputPath(initConf, new Path(op));
		RunningJob job1 = JobClient.runJob(initConf);
		job1.waitForCompletion();
		
		// for the second round of map-red
		int iterationCount = 0;

		while (keepGoing(iterationCount)) {			
			IS_ALL_BLACK = true;
			
			String input;
			if (iterationCount == 0)
				input = INPUT1_FILE_LOCATION;
			else
				input = OUTPUT_FILE_LOCATION + iterationCount;

			String output = OUTPUT_FILE_LOCATION + (iterationCount + 1);

			JobConf conf = getJobConf(args);
			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(output));
			RunningJob job = JobClient.runJob(conf);

			iterationCount++;
			
			System.out.println("\nIS ALL BLACK ? "+ IS_ALL_BLACK+"\n");
		}

		return 0;
	}

	private boolean keepGoing(int iterationCount) {
	/*
		if (iterationCount >= 4) {
			return false;
		}
	 */
		
		if(IS_ALL_BLACK)
			return false;
		
		return true;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RoadNetwork(), args);
		System.exit(res);
	}

	/***
	 * Execute Shell command for merging files
	 ****/
	private String executeCommand(String command) {

		StringBuffer output = new StringBuffer();

		Process p;
		try {
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					p.getInputStream()));

			String line = "";
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return output.toString();

	}

}
