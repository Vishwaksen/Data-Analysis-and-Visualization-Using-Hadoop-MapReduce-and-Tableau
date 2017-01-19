/* Hadoop Map Reduce Program to compute the year which offered maximum number of courses. */ 

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

public class question2 {

  public static class Mapper1
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String[] fields=value.toString().split(","); //new array of 9 elements
    	if(fields[1].equals("Unknown")||fields[2].equals("Unknown")||fields[7].equals("")||fields[8].equals("")||fields[8].equals("0")||!StringUtils.isNumeric(fields[7])||!StringUtils.isNumeric(fields[8]))return;
    	word.set(fields[1].split(" ")[1]);
    	context.write(word,new IntWritable(1));
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
  extends Mapper<Object, Text, IntWritable, Text>{

private Text word = new Text();

public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	String[] fields=value.toString().split("\\t");
	String swapped_val = fields[0];
	IntWritable swapped_key = new IntWritable(Integer.parseInt(fields[1]));
	Text swapped_value = new Text(swapped_val);
	context.write(swapped_key, swapped_value);
}
}

public static class Reducer2
  extends Reducer<IntWritable,Text,IntWritable, Text> {
//private IntWritable result = new IntWritable();
  Text result = new Text();
public void reduce(IntWritable key, Iterable<Text> values,
                  Context context
                  ) throws IOException, InterruptedException {
	Iterator<Text> itr=values.iterator();
	context.write(key,itr.next());
}
}
  
  public static class Mapper3
  extends Mapper<Object, Text, Text, Text>{
  	Text word=new Text();

public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	Text txt = new Text("DUMMY TEXT");
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
	while(itr.hasNext()) {
		res = itr.next();
	}
	String strng = res.toString();
	String[] fields = strng.split("\\t");
	String tmp = fields[1] + "  " + fields[0];
	Text val = new Text(tmp);
	Text dummy = new Text("");
	context.write(dummy,val);
}
}
  public static void main(String[] args) throws Exception {
	String temp="Temp";
	String temp1="Temp1";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "get total number of courses offered per year");
    job.setJarByClass(question2.class);
    job.setMapperClass(Mapper1.class);
    job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(temp));
    job.waitForCompletion(true);
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "swap key value pairs");
    job2.setJarByClass(question2.class);
    job2.setMapperClass(Mapper2.class);
    //job2.setCombinerClass(FindIncreaseReducer.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path("Temp"));
    FileOutputFormat.setOutputPath(job2, new Path(temp1));
    job2.waitForCompletion(true);
    Configuration conf3 = new Configuration();
    Job job3 = Job.getInstance(conf3, "Get year that offers minimum number of courses");
    job3.setJarByClass(question2.class);
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