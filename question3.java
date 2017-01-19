/* Hadoop Map Reduce Program to compute vacant/unfilled/empty seats for each Building */

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

public class question3 {
	
  public static class Mapper1
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String[] fields=value.toString().split(","); //new array of 9 elements
    	if(fields[1].equals("Unknown")||fields[2].equals("Unknown")||fields[7].equals("")||fields[8].equals("")||fields[8].equals("0")||!StringUtils.isNumeric(fields[7])||!StringUtils.isNumeric(fields[8]))return;
    	word.set(fields[2].split(" ")[0]);
    	IntWritable val1  = new IntWritable(Integer.parseInt(fields[7]));
    	context.write(word,val1);
     	IntWritable val2  = new IntWritable(Integer.parseInt(fields[8]));
     	word.set(fields[2].split(" ")[0] + "_total");
    	context.write(word,val2);
    }
  }

  public static class Reducer1
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	int sum=0;
    	for(IntWritable val:values){
    		sum+=val.get();
    	}
    	result.set(sum);
    	context.write(key,result);
    }
  }
  public static class Mapper2
  extends Mapper<Object, Text, Text, IntWritable>{
  	Text word=new Text();

public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	String[] fields=value.toString().split("\\t");  //new array of 9 elements
	if(!(fields[0].contains("_total"))) {
	word.set(fields[0]);
	IntWritable val1  = new IntWritable(Integer.parseInt(fields[1]));
	context.write(word,val1);
	}
	else {
	word.set(fields[0].substring(0, fields[0].length()-6));
	IntWritable val2  = new IntWritable(Integer.parseInt(fields[1]));
	context.write(word,val2);
	}
}
}

public static class Reducer2
  extends Reducer<Text,IntWritable,Text,IntWritable> {

public void reduce(Text key, Iterable<IntWritable> values,
                  Context context
                  ) throws IOException, InterruptedException {
	int z;
	Iterator<IntWritable> iterator=values.iterator();
	int value1=iterator.next().get();
	if(!iterator.hasNext())return;
	int value2=iterator.next().get();
	if(value1>value2)
	 z = value1-value2;
	else
	 z = value2 - value1;
	IntWritable w = new IntWritable(z);
	context.write(key,w);
}
}
  public static void main(String[] args) throws Exception {
	String temp="Temp";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "get total number of enrollments for every building");
    job.setJarByClass(question3.class);
    job.setMapperClass(Mapper1.class);
    job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(temp));
    job.waitForCompletion(true);
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "get utilization rate for each building");
    job2.setJarByClass(question3.class);
    job2.setMapperClass(Mapper2.class);
    //job2.setCombinerClass(FindIncreaseReducer.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path("Temp"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
    
  }
}