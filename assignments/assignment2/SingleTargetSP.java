package comp9313.ass2;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SingleTargetSP {
	
	public static enum ST_COUNTER {
		UPDATE
	};
	public static int targetNodeID = 0;
	public static String IN = "input";
	public static String OUT = "output";
	
	public static class STMapper extends Mapper<Object, Text, IntWritable, Text> {
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer it = new StringTokenizer(value.toString());
			while (it.hasMoreTokens()) {
				String node = it.nextToken(), distance = it.nextToken(), path = it.nextToken(), adjacencyList = it.nextToken();
				
				// Emit the original node parameters
				context.write(new IntWritable(Integer.parseInt(node)), new Text(distance + "\t" + path + "\t" + adjacencyList));
				
				// Emit new distance and path of adjacent nodes
				if (!distance.equals("-") && !adjacencyList.equals("NONE")) {
					String[] adjacentNodes = adjacencyList.split(",");
					for (String adjacentNode : adjacentNodes) {
						String[] tokens = adjacentNode.split(":");
						double newDistance = Double.parseDouble(distance) + Double.parseDouble(tokens[1]);
						String newPath = tokens[0] + "->" + path;
						context.write(new IntWritable(Integer.parseInt(tokens[0])), new Text(newDistance + "\t" + newPath + "\t-"));
					}
				}
			}
		}
		
	}
	
	
	public static class FinalMapper extends Mapper<Object, Text, IntWritable, Text> {
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer it = new StringTokenizer(value.toString());
			while (it.hasMoreTokens()) {
				@SuppressWarnings("unused")
				String node = it.nextToken(), distance = it.nextToken(), path = it.nextToken(), adjacencyList = it.nextToken();
				
				// Emit only the node ID, distance, and path
				context.write(new IntWritable(Integer.parseInt(node)), new Text(distance + "\t" + path));
			}
		}
		
	}
	
	
	public static class STReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String distance = "-", newDistance = "-", path = "-", adjacencyList = "NONE";
			
			for (Text value : values) {
				String[] tokens = value.toString().split("\t");
				// Update distance and path if a shorter path is found
				if (newDistance.equals("-") || (!tokens[0].equals("-") && Double.parseDouble(tokens[0]) < Double.parseDouble(newDistance))) {
					newDistance = tokens[0];
					path = tokens[1];
				}
				// Save distance and adjacent nodes information of the original node
				if (!tokens[2].equals("-")) {
					distance = tokens[0];
					adjacencyList = tokens[2];
				}
			}
			
			// Increment counter if a shorter path is found
			if (!newDistance.equals(distance)) {
				context.getCounter(ST_COUNTER.UPDATE).increment(1);
			}
			
			context.write(key, new Text(newDistance + "\t" + path + "\t" + adjacencyList));
		}
		
	}
	
	
	public static class FinalReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Emit only nodes that can reach the target node
			for (Text value : values) {
				String[] tokens = value.toString().split("\t");
				if (!tokens[1].equals("-")) {
					context.write(key, new Text(tokens[0] + "\t" + tokens[1]));
				}
			}
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		IN = args[0];
		OUT = args[1];
		targetNodeID = Integer.parseInt(args[2]);
		
		FileSystem hdfs = FileSystem.get(URI.create(IN), new Configuration());
		FileStatus[] fileStatuses = hdfs.listStatus(new Path(IN));
		for(FileStatus fileStatus : fileStatuses) {
			Path inputFilePath = fileStatus.getPath();
			
			int iteration = 0;
			String input = IN;
			String output = OUT + iteration;
			
			// Convert the input file to the desired format for iteration
			// NodeID\tDistance\tPath\tAdjacentNodes
			// AdjacentNodes contains information of Nodes that can reach NodeID
			FSDataInputStream fsDataInputStream = hdfs.open(inputFilePath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));
			Map<Integer, String> map = new HashMap<Integer, String>();
			String line = null;
			while((line = br.readLine())!= null){
				String[] values = line.split(" ");
				if (values.length != 4) {
					break;
				}
				int toNodeID = Integer.parseInt(values[2]);
				if (map.containsKey(toNodeID)) {
					map.put(toNodeID, map.get(toNodeID) + "," + values[1] + ":" + values[3]);
				}
				else if (toNodeID == targetNodeID) {
					map.put(toNodeID, "0.0\t" + targetNodeID + "\t" + values[1] + ":" + values[3]);
				}
				else {
					map.put(toNodeID, "-\t-\t" + values[1] + ":" + values[3]);
				}
			}
			
			// map will be empty if the input file is incorrect
			if (!map.isEmpty()) {
				// Save the updated input file
				Path outputFilePath = new Path(output + "/" + FilenameUtils.getName(inputFilePath.toString()));
				FSDataOutputStream fsDataOutputStream = hdfs.create(outputFilePath);
				Iterator<Entry<Integer, String>> it = map.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry<Integer, String> pair = (Map.Entry<Integer, String>)it.next();
					fsDataOutputStream.writeBytes(pair.getKey() + "\t" + pair.getValue() + "\n");
					it.remove();
				}
				fsDataOutputStream.close();
				
				boolean isDone = false;
				while (isDone == false) {
					// Configure and run the MapReduce job
					input = output;
					iteration ++;
					output = OUT + iteration;
					
					Job job = Job.getInstance(new Configuration(), "Single Target Shortest Path");
					job.setJarByClass(SingleTargetSP.class);
					
					job.setNumReduceTasks(1);
					
					job.setMapperClass(STMapper.class);
					job.setReducerClass(STReducer.class);
					
					job.setOutputKeyClass(IntWritable.class);
					job.setOutputValueClass(Text.class);
					
					FileInputFormat.addInputPath(job, new Path(input));
					FileOutputFormat.setOutputPath(job, new Path(output));
					
					job.waitForCompletion(true);
					
					// Delete the input folder to save disk space
					FileSystem hdfs1 = FileSystem.get(new URI(input), new Configuration());
					hdfs1.delete(new Path(input), true);
					
					// Check the termination criterion by utilizing the counter
					Counters counters = job.getCounters();
					Counter update_counter = counters.findCounter(ST_COUNTER.UPDATE);
					if (update_counter.getValue() == 0) {
						isDone = true;
					}
				}
				
				// Extract the final result using another MapReduce job with only 1 reducer, and store the results in HDFS
				Job job = Job.getInstance(new Configuration(), "Single Target Shortest Path");
				job.setJarByClass(SingleTargetSP.class);
				
				job.setNumReduceTasks(1);
				
				job.setMapperClass(FinalMapper.class);
				job.setReducerClass(FinalReducer.class);
				
				job.setOutputKeyClass(IntWritable.class);
				job.setOutputValueClass(Text.class);
				
				FileInputFormat.addInputPath(job, new Path(output));
				FileOutputFormat.setOutputPath(job, new Path(OUT));
				
				job.waitForCompletion(true);
				
				// Delete the input folder to save disk space
				FileSystem hdfs1 = FileSystem.get(new URI(output), new Configuration());
				hdfs1.delete(new Path(output), true);
			}
			
			fsDataInputStream.close();
			br.close();
		}
	}

}
