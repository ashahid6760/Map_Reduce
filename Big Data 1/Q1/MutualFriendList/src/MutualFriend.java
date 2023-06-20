import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class MutualFriend {	
	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] users = values.toString().split("\t");
			if (users.length == 2) {
				int user1_id = Integer.parseInt(users[0]);
				String[] friends_list = users[1].split(",");
				int user2_id;
				Text TupleKey = new Text();
				
				for (String user2 : friends_list) {
					user2_id = Integer.parseInt(user2);
					if (user1_id < user2_id) {
						TupleKey.set(user1_id + "," + user2_id);
					} else {
						TupleKey.set(user2_id + "," + user1_id);
					}
					context.write(TupleKey, new Text(users[1]));
				}
			}
		}
	}
	
	public static class ReducerClass extends Reducer<Text, Text, Text,Text> {
		Text final_value = new Text();    	
		public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder mutual_friends_list = new StringBuilder();
			HashMap<String, Integer> hashMap = new HashMap<String,Integer>();
			for (Text tuples : values) {
				String[] friends_list = tuples.toString().split(",");
				for (String friend : friends_list) {
					if(hashMap.containsKey(friend)){
						mutual_friends_list.append(friend + ',' );
					}
					else {
						hashMap.put(friend, 1);
					}
				}
			}
				if(mutual_friends_list.lastIndexOf(",") > -1) {
					mutual_friends_list.deleteCharAt(mutual_friends_list.lastIndexOf(","));
				}				
				final_value.set(new Text(mutual_friends_list.toString()));
				context.write(key,final_value);
			}
		}
	
	
	public static void main(String[] args) throws Exception{
		if(args.length!=2) {
			System.out.println("I/p :user i/p, o/p path");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		@SuppressWarnings("depreciation")
		Job job = Job.getInstance(conf, "MutualFriend");
		job.setJarByClass(MutualFriend.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		if (!job.waitForCompletion(true)) {
			System.exit(1);
		}
	}	
}
