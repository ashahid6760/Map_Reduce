import java.io.BufferedReader;
//import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
//import java.nio.file.FileSystem;
import java.time.LocalDate;
import java.time.Period;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
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

public class MapperJoin {
	
	public static class MapperClass extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		Map<String, String> user_data_map = new HashMap<>();
		private final Text user = new Text();
		private final Text friendsList = new Text();
		
		public int calculateAge(String id) {
			String dateOfBirth = user_data_map.get(id);
			String[] iput = dateOfBirth.split("/");
			LocalDate bday = LocalDate.of(Integer.parseInt(iput[2]), Integer.parseInt(iput[0]), Integer.parseInt(iput[1]));
			LocalDate current_date = LocalDate.of(2023, 01, 01);
			Period p = Period.between(bday, current_date);
			return p.getYears();
		}
		
		public void setup(Mapper.Context context) throws IOException, InterruptedException {
			super.setup(context);
			 @SuppressWarnings("deprecation")
			 Configuration conf = context.getConfiguration();
	         Path fPath = new Path(conf.get("mapper.input"));
	         FileSystem fs = FileSystem.get(conf);
	            FileStatus[] status = fs.listStatus(fPath);
	            for(FileStatus s : status)
	            {
	                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(s.getPath())));
	                String entire_line;
	                entire_line = br.readLine();
	                while (entire_line != null) 
	                {
	                    String[] array = entire_line.split(",");
	                    if (array.length == 10)
	                    	user_data_map.put(array[0], array[9]);
	                    entire_line = br.readLine();
	                }
	            }
	        }		

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] users = values.toString().split("\t");
			if (users.length == 2) {
				int user1_id = Integer.parseInt(users[0]);
				String[] friends_list = users[1].split(",");
				int user2_id;
				Text tupleKey = new Text();
				for (String user_2 : friends_list) {
					user2_id = Integer.parseInt(user_2);
					int age = calculateAge(user_2);
					context.write(new IntWritable(user1_id), new IntWritable(age));
				}
			}
		}
	}
	
	public static class ReducerClass extends Reducer<IntWritable, IntWritable, IntWritable,DoubleWritable> {
		public void reduce (IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int age_sum = 0;
			int friends_count = 0;
			for (IntWritable age : values) {
				age_sum += age.get();
				friends_count += 1;
			}
			DoubleWritable average_age = new DoubleWritable( (double) age_sum / friends_count);
		    context.write(key,average_age);
		}
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length!=3) {
			System.out.println("I/p order : user i/p, mutual friends data path, o/p path");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		conf.set("mapper.input", args[0]);
		@SuppressWarnings("depreciation")
		Job job = Job.getInstance(conf, "MapperJoin");
		job.setJarByClass(MapperJoin.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		if (!job.waitForCompletion(true)) {
			System.exit(1);
		}
	}	
}
