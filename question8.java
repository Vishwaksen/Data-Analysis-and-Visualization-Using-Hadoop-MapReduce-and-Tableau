//Hadoop Map-Reduce Program to Compute the year in which Capen Hall held the minimum number of courses? 

/*
  Author : Vishwaksen Mane 
*/


import java.io.IOException;
import java.util.Iterator;
import java.lang.Math;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import question2.Mapper3;
//import question2.Reducer3;

public class question8 
{
	public static class MyMapper1 extends Mapper<Object, Text, Text, IntWritable>
	{
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
    			String[] fields = value.toString().split(","); 
			String semesterID = fields[0]; 
			String semester = fields[1]; 
			String hall = fields[2];
			//String weekdays = fields[3];
			//String classTime = fields[4];
			String courseID = fields[5]; 
			String courseName = fields[6]; 
			//String acturalEnrollment = fields[7]; 
			//String maxEnrollment = fields[8];

			String halls = hall.split(" ")[0]; 
			String year = fields[1].split(" ")[1];  
    			
			if(fields[1].equals("Unknown")||!fields[2].contains("Capen")||
				fields[5].equals("")||fields[7].equals("")||!StringUtils.isNumeric(fields[7]))return;
			
			String keys = halls +" " + year;  
    			word.set(keys);
    			//System.out.println(keys); 
			context.write(word, new IntWritable(1));
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
			//System.out.println(sum); 
    		}
	}

  	public static class MyMapper2 extends Mapper<Object, Text, IntWritable, Text>
	{
  		Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
         String[] fields = value.toString().split("\\t");
         IntWritable key1 = new IntWritable(Integer.parseInt(fields[1]));
         Text txt1 = new Text(fields[0]);
		 context.write(key1,txt1);
		}
	}

	public static class MyReducer2 extends Reducer<IntWritable,Text,IntWritable,Text> 
	{	
		int max=Integer.MIN_VALUE;
		Text current=null;
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{	
			Iterator<Text> itr=values.iterator();
			context.write(key,itr.next());
		}	
	}
	
	 public static class Mapper3
	  extends Mapper<Object, Text, Text, Text>{
	  	Text word=new Text();

	public void map(Object key, Text value, Context context
	               ) throws IOException, InterruptedException {
		Text txt = new Text("SAMPLE");
		Text res = new Text(value.toString());
		context.write(txt, res);
	}
	}

	public static class Reducer3
	  extends Reducer<Text,Text,Text,Text> {
		private IntWritable result = new IntWritable();
	public void reduce(Text key, Iterable<Text> values,
	                  Context context
	                  ) throws IOException, InterruptedException {
		Iterator <Text> itr = values.iterator();
		Text res = new Text("");
		Text res1 = new Text("");
		int i=0;
		res1=itr.next();
		/*while(itr.hasNext()) {
            if(i==0)
            	res1=itr.next();
            else
			res = itr.next();
            i=i+1;
		} */
		String strng = res1.toString();
		String[] fields = strng.split("\\t");
		String tmp = fields[1] + "  " + fields[0];
		Text val = new Text(tmp);
		Text dummy = new Text("");
		context.write(dummy,val);
	}
	}

	public static void main(String[] args) throws Exception 
	{
		String temp="Temp";
		String temp1="Temp1";
    		Configuration conf = new Configuration();
    		Job job = Job.getInstance(conf, "Get the total number of actual enrollment each year");
    		job.setJarByClass(question8.class);
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
    		job2.setJarByClass(question8.class);
    		job2.setMapperClass(MyMapper2.class);
    		job2.setReducerClass(MyReducer2.class);
    		job2.setOutputKeyClass(IntWritable.class);
    		job2.setOutputValueClass(Text.class);
    		FileInputFormat.addInputPath(job2, new Path("Temp"));
    		FileOutputFormat.setOutputPath(job2, new Path(temp1));
    	    job2.waitForCompletion(true);
    	    Configuration conf3 = new Configuration();
    	    Job job3 = Job.getInstance(conf3, "Get year in which Capen offers minimum number of courses");
    	    job3.setJarByClass(question8.class);
    	    job3.setMapperClass(Mapper3.class);
    	    //job2.setCombinerClass(FindIncreaseReducer.class);
    	    job3.setReducerClass(Reducer3.class);
    	    job3.setOutputKeyClass(Text.class);
    	    job3.setOutputValueClass(Text.class);
    	    FileInputFormat.addInputPath(job3, new Path("Temp1"));
    	    FileOutputFormat.setOutputPath(job3, new Path(args[1]));
    	    System.exit(job3.waitForCompletion(true) ? 0 : 1);
    	}
}
