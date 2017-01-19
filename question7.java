//Hadoop Map-Reduce Program to compute the trend of actual enrollment of students in Baldy Hall after 2010? 

/*
  Author : Vishwaksen Mane 
*/

import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class question7 
{
	public static class MyMapper1 extends Mapper<Object, Text, Text, IntWritable>
	{
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
    			String[] fields = value.toString().split(","); 
			//String semesterID = fields[0]; 
			//String semester = fields[1]; 
			String hall = fields[2];
			//String weekdays = fields[3];
			//String classTime = fields[4];
			//String courseID = fields[5]; 
			//String courseName = fields[6]; 
			//String acturalEnrollment = fields[7]; 
			//String maxEnrollment = fields[8];

			String[] halls = hall.split(" "); 
			String[] year = fields[1].split(" ");  
    			
			if(fields[1].equals("Unknown")||!fields[2].contains("Baldy")||fields[7].equals("")||!StringUtils.isNumeric(fields[7]))
			return;
			
			String keys = halls[0] + " " + year[1];  
    			word.set(keys);
    			
			context.write(word, new IntWritable(Integer.parseInt(fields[7])));
    		}
	}

  	public static class MyReducer1 extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
    		private IntWritable result = new IntWritable();

    		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
    			int sum=0;
    			for(IntWritable val:values)
			{
    				sum+=val.get();
    			}
    			result.set(sum);
    			context.write(key,result);
    		}
	}

  	public static class MyMapper2 extends Mapper<Object, Text, Text, IntWritable>
	{
  		Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{

			String[] fields = value.toString().split("\\t"); 
			String[] hallsandyear = fields[0].split(" ");
			String hall = hallsandyear[0];
			String year = hallsandyear[1];
			String key1 = hall+" "+year+"-"+(Integer.parseInt(year)+1);
			String key2 = hall+" "+Integer.toString(Integer.parseInt(year)-1)+"-"+year;

			word.set(key1);
			context.write(word,new IntWritable(Integer.parseInt(fields[1])));
			word.set(key2);
			context.write(word,new IntWritable(Integer.parseInt(fields[1])));
		}
	}

	public static class MyReducer2 extends Reducer<Text,IntWritable,Text,IntWritable> 
	{	

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{	
			Iterator<IntWritable> iterator = values.iterator();
			int value1 = iterator.next().get(); 
			if (!iterator.hasNext()) return; 
			int value2 = iterator.next().get(); 
			value2 = value2 - value1; 
			context.write(key, new IntWritable(value2)); 
		}	
	}

	public static void main(String[] args) throws Exception 
	{
		String temp="Temp";
    		Configuration conf = new Configuration();
    		Job job = Job.getInstance(conf, "Get the total number of actual enrollment each year");
    		job.setJarByClass(question7.class);
    		job.setMapperClass(MyMapper1.class);
    		job.setCombinerClass(MyReducer1.class);
    		job.setReducerClass(MyReducer1.class);
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(IntWritable.class);
    		FileInputFormat.addInputPath(job, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job, new Path(temp));
    		job.waitForCompletion(true);
    		Configuration conf2 = new Configuration();
    		Job job2 = Job.getInstance(conf2, "get the trend of the differences of actual enrollment students between years");
    		job2.setJarByClass(question7.class);
    		job2.setMapperClass(MyMapper2.class);
    		job2.setReducerClass(MyReducer2.class);
    		job2.setOutputKeyClass(Text.class);
    		job2.setOutputValueClass(IntWritable.class);
    		FileInputFormat.addInputPath(job2, new Path("Temp"));
    		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    		System.exit(job2.waitForCompletion(true) ? 0 : 1);
    	}
}