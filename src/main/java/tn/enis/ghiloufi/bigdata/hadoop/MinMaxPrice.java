package tn.enis.ghiloufi.bigdata.hadoop;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MinMaxPrice extends Configured implements Tool {


public static void main(String[] args) throws Exception {
	
	int exitCode = ToolRunner.run(new MinMaxPrice(), args);
	System.exit(exitCode);
   
}

@Override
public int run(String[] args) throws Exception {
	
	if (args.length != 2) {
		System.err.printf("NOTE:il faut  deux entrées <input> <output>",getClass().getSimpleName());
		return -1;
	}
	
	//Initialize the Hadoop job and set the jar as well as the name of the Job
	@SuppressWarnings("deprecation")
	Job job = new Job();
	job.setJarByClass(MinMaxPrice.class);
	job.setJobName("MIN_MAX_PRICE");

	//Add input and output file paths to job based on the arguments passed
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(FloatWritable.class);
	job.setOutputFormatClass(TextOutputFormat.class);
			
	//Set the MapperClass and ReduceClass in the job
	job.setMapperClass(MapperClass.class);
	job.setReducerClass(ReduceClass.class);
		
	//Wait for the job to complete and print if the job was successful or not
	int returnValue = job.waitForCompletion(true) ? 0:1;
			
	if(job.isSuccessful()) {
		System.out.println("Job was successful");
	} else if(!job.isSuccessful()) {
		System.out.println("Job was not successful");			
	}
			
	return returnValue;
}

}