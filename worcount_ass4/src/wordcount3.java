package wordcount;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.PropertyConfigurator;

public class wordcount3 {	
		public static class Mapper_Part1 extends Mapper<LongWritable, Text, Text, Text> {
		String File_name = ""; 
		int all = 0;  
		static Text one = new Text("1");
		String word;
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			String str = split.getPath().toString();
			File_name = str.substring(str.lastIndexOf("/")+1);
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word = File_name ;
				word += " ";
				word += itr.nextToken(); 
				all++;
				context.write(new Text(word), one);
				}
		}
		public void cleanup(Context context) throws IOException,
		InterruptedException {
			String str = "";
			str += all;
			context.write(new Text(File_name + " " + "!"), new Text(str));
		}
	}
		public static class Combiner_Part1 extends Reducer<Text, Text, Text, Text> {
		float all = 0;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int index = key.toString().indexOf(" ");
			if (key.toString().substring(index + 1, index + 2).equals("!")){
			for (Text val : values) {
					all = Integer.parseInt(val.toString());
				}
				return;
			}
			float sum = 0; 
			for (Text val : values) {
				sum += Integer.parseInt(val.toString());
			}
			float tmp = sum / all;
			String value = "";
			value += tmp;  
			
			String p[] = key.toString().split(" ");
			String key_to = "";
			
			key_to += p[1];
			key_to += " ";
			key_to += p[0];
			
			context.write(new Text(key_to), new Text(value));
		}
	}
		public static class Reduce_Part1 extends Reducer<Text, Text, Text, Text> {
		
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				
				context.write(key, val);
			}
		}
	}
	
	public static class MyPartitoner extends Partitioner<Text, Text>{

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String ip1 = key.toString();
			ip1 = ip1.substring(0, ip1.indexOf(" "));
			Text p1 = new Text(ip1);
		return Math.abs((p1.hashCode() * 127) % numPartitions);
		}
}

	public static  class Mapper_Part2 extends
	Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
		String val = value.toString().replaceAll("	", " ");
		int index = val.indexOf(" ");
		String s1 = val.substring(0, index); 
		String s2 = val.substring(index + 1); 
		s2 += " ";
		s2 += "1"; 
		context.write(new Text(s1), new Text(s2));
		}
	}
public static  class Reduce_Part2 extends
	Reducer<Text, Text, Text, Text>{
	int file_count;
	public void reduce(Text key, Iterable<Text>values, Context context)throws IOException, InterruptedException{
		file_count = context.getNumReduceTasks();  
		float sum = 0;
		List<String> vals = new ArrayList<String>();
		for (Text str : values){
			int index = str.toString().lastIndexOf(" ");
			sum += Integer.parseInt(str.toString().substring(index + 1));
			vals.add(str.toString().substring(0, index));
		}
		float tmp = sum / file_count; 
		for (int j = 0;j < vals.size(); j++){
			String val = vals.get(j);
			String end = val.substring(val.lastIndexOf(" "));
			float f_end = Float.parseFloat(end);
			val += " ";
			val += tmp;
			val += " ";
			val += f_end / tmp;  
			context.write(key, new Text(val));
		}
	}
}
	public static void main(String[] args) throws Exception {

		Path tmp = new Path("hdfs://arrow4g:9000/data/ass4_output/3_output"); 

		PropertyConfigurator.configure("config/log4j.properties");
		Configuration conf1 = new Configuration();

		FileSystem hdfs = FileSystem.get(conf1);
		FileStatus p[] = hdfs.listStatus(new Path("config/log4j.properties"));
	
		Job job1 = new Job(conf1, "My_tdif_part1");
		
		job1.setJarByClass(wordcount3.class);
		
		job1.setMapperClass(Mapper_Part1.class);
		job1.setCombinerClass(Combiner_Part1.class); 
		job1.setReducerClass(Reduce_Part1.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
	
		job1.setNumReduceTasks(p.length);
		
		job1.setPartitionerClass(MyPartitoner.class); 
		
		FileInputFormat.addInputPath(job1, new Path("hdfs://arrow4g:9000/data/ass4/"));
		
		FileOutputFormat.setOutputPath(job1, tmp);
		
		job1.waitForCompletion(true);

		Configuration conf2 = new Configuration();
	
		Job job2 = new Job(conf2, "My_tdif_part2");
		
		job2.setJarByClass(wordcount3.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setMapperClass(Mapper_Part2.class);
		job2.setReducerClass(Reduce_Part2.class);

		job2.setNumReduceTasks(p.length);
		
		FileInputFormat.setInputPaths(job2, tmp);
		FileOutputFormat.setOutputPath(job2, new Path("hdfs://arrow4g:9000/data/ass4_output/3_output2"));
	}
}