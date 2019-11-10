package wordcount;
import java.io.IOException; 
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.PropertyConfigurator; 
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class wordcount {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{ 
		private final static IntWritable one = new IntWritable(1); 
		private Text word = new Text(); 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{ 
			StringTokenizer itr= new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) { 
				word.set(itr.nextToken().toLowerCase().replaceAll("[^0-9a-zA-Z\u4e00-\u9fa5]"," ")); 
				context.write(word, one); 
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable(); 
		public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException{ 
			int sum = 0; 
			for (IntWritable val: values) { 
				sum += val.get(); 
			} 
			result.set(sum); 
			context.write(key, result); 
		}
	}
	
	public static void main(String[] args) throws Exception { 
		long startTime = System.currentTimeMillis();
		PropertyConfigurator.configure("config/log4j.properties");
		Configuration conf= new Configuration(); 
		Job job= Job.getInstance(conf, "word count"); 
		job.setJarByClass(wordcount.class);
		job.setMapperClass(TokenizerMapper.class); 
		job.setCombinerClass(IntSumReducer.class); 
		job.setReducerClass(IntSumReducer.class); 
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://arrow4g:9000/data/ass4/")); 
		FileOutputFormat.setOutputPath(job, new Path("hdfs://arrow4g:9000/data/ass4_output/1_output")); 
		System.exit(job.waitForCompletion(true) ? 0 : 1); 
		long endTime = System.currentTimeMillis();
		System.out.println("Running time: " + (endTime - startTime) + "ms");
	}
}
