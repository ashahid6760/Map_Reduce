import java.io.*;
import java.io.IOException;
//import java.nio.file.FileSystem;
import java.time.LocalDate;
import java.time.Period;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
public class Reducer_Join {
	
	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] users_data = values.toString().split("\t");
			if (users_data.length == 2) {
				int user1_id = Integer.parseInt(users_data[0]);
				String[] friends_list = users_data[1].split(",");
				int user2_id;
				Text tupleKey = new Text();
				for (String friend : friends_list) {
					user2_id = Integer.parseInt(friend);
					if (user1_id < user2_id) {
						tupleKey.set(user1_id + "," + user2_id);
					} else {
						tupleKey.set(user2_id + "," + user1_id);
					}
					context.write(tupleKey, new Text(users_data[1]));
				}
			}
		}
	}
	
	public static class ReducerClass extends Reducer<Text, Text, Text,Text> {
		Map<String, String> user_map = new HashMap<>();
		public int calculateAge(String id) {
			String dateOfBirth = user_map.get(id);
			String[] iput = dateOfBirth.split("/");
			LocalDate bday = LocalDate.of(Integer.parseInt(iput[2]), Integer.parseInt(iput[0]), Integer.parseInt(iput[1]));
			LocalDate current_day = LocalDate.of(2023, 01, 01);
			Period p = Period.between(bday, current_day);
			return p.getYears();
		}
		
		@Override
		public void setup(Reducer.Context context) throws IOException, InterruptedException {
			super.setup(context);
			 @SuppressWarnings("deprecation")
			 Configuration conf = context.getConfiguration();
	         Path fPath = new Path(conf.get("reduce.input"));
	         FileSystem fs = FileSystem.get(conf);
	            FileStatus[] status = fs.listStatus(fPath);
	            for(FileStatus s : status)
	            {
	                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(s.getPath())));
	                String line;
	                line = br.readLine();
	                while (line != null) 
	                {
	                    String[] array = line.split(",");
	                    if (array.length == 10)
	                    	user_map.put(array[0], array[9]);
	                    line = br.readLine();
	                }
	            }
	        }
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			Text result=new Text();
			StringBuilder mutual_friends_array = new StringBuilder();
			mutual_friends_array.append("[");
			int minimumAge  = Integer.MAX_VALUE;
			HashMap<String, Integer> hashMap = new HashMap<String,Integer>();
			for (Text tuples : values) {
				String[] friends_list = tuples.toString().split(",");
				for (String friend : friends_list) {
					if(hashMap.containsKey(friend)){
						mutual_friends_array.append(friend + ",");
						minimumAge = Math.min(minimumAge, calculateAge(friend));
						}
					else {
						hashMap.put(friend, 1);
					}
				}
		   }
			if(mutual_friends_array.lastIndexOf(",") > -1) {
				mutual_friends_array.deleteCharAt(mutual_friends_array.lastIndexOf(","));
				mutual_friends_array.append("]");
				mutual_friends_array.append(" "+minimumAge);
			}
			result.set(new Text(mutual_friends_array.toString()));
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length!=3) {
			System.out.println(" i/p order : user i/p, mutual friends data path, o/p path location");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		conf.set("reduce.input", args[0]);
		@SuppressWarnings("depreciation")
		Job job = Job.getInstance(conf);
		job.setJarByClass(Reducer_Join.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		if (!job.waitForCompletion(true)) {
			System.exit(1);
		}
	}	
		
}


