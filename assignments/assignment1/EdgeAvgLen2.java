package comp9313.ass1;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EdgeAvgLen2 {
	
	// Mapper Class
	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text> {
		Map<Integer, String> map = new HashMap<Integer, String>();
		
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				// Assumes each line in the input file is formated as specified "EdgeId FromNodeId ToNodeId Distance"
				@SuppressWarnings("unused")
				int EdgeId = Integer.parseInt(itr.nextToken());
				@SuppressWarnings("unused")
				int FromNodeId = Integer.parseInt(itr.nextToken());
				int ToNodeId = Integer.parseInt(itr.nextToken());
				double Distance = Double.parseDouble(itr.nextToken());
				
				// Only need to keep track of in-coming edges
				// Calculate the total length and number of in-coming edges
				if (map.containsKey(ToNodeId)) {
					String tokens[] = map.get(ToNodeId).split("_");
					double totalLength = Double.parseDouble(tokens[0]) + Distance;
					int nEdges = Integer.parseInt(tokens[1]) + 1;
					map.put(ToNodeId, Double.toString(totalLength) + "_" + Integer.toString(nEdges));
				}
				else {
					map.put(ToNodeId, Double.toString(Distance) + "_1");
				}
			}
		}
		
		public void cleanup(Context context) 
				throws IOException, InterruptedException {
			Iterator<Map.Entry<Integer, String>> itr = map.entrySet().iterator();
			while (itr.hasNext()) {
				// Calculate the average length of in-coming edges
				Map.Entry<Integer, String> entry = itr.next();
				String tokens[] = entry.getValue().split("_");
				double totalLength = Double.parseDouble(tokens[0]);
				int nEdges = Integer.parseInt(tokens[1]);
				double average = totalLength / nEdges;
				
				// Need to keep track of the number of in-coming edges for the Reducer to calculate average
				// <k, v>; k = NodeId, v = AverageLength_NumberOfInComingEdges
				context.write(new IntWritable(entry.getKey()), new Text(average + "_" + nEdges));
			}
		}
	}
	
	// Reducer Class
	public static class AverageReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double average = 0D, totalLength = 0D;
			int count = 0, nEdges = 0;
			
			// Calculate the average length of in-coming edges
			for (Text value : values) {
				String[] tokens = value.toString().split("_");
				average = Double.parseDouble(tokens[0]);
				count = Integer.parseInt(tokens[1]);
				totalLength += (average * count);
				nEdges += count;
			}
			average = totalLength / nEdges;
			
			// The number of in-coming edges is no longer needed
			context.write(key, new Text(Double.toString(average)));
		}
	}
	
	// Main Function
	public static void main(String[] args) 
			throws Exception {
		Job job = Job.getInstance(new Configuration(), "Edge Average Length");
		job.setJarByClass(EdgeAvgLen2.class);
		
		job.setNumReduceTasks(1);
		
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(AverageReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}