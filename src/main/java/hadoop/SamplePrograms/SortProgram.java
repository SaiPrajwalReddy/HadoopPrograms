package hadoop.SamplePrograms;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortProgram extends Configured implements Tool {
	
	static class SortMapper extends Mapper<IntWritable,Text,IntWritable,Text>
	{
		public void map(IntWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			int temperature = 0;
			String year = "";
			String record = value.toString();
			String airQuality = record.substring(92,93);
			year = record.substring(15, 19);

			if (record.charAt(87) == '+' || record.charAt(87) == '-') {
				temperature = Integer.parseInt(record.substring(88, 92));
			} else {
				temperature = Integer.parseInt(record.substring(87, 92));
			}
			
			if(9999 != temperature && airQuality.matches("[01459]"))		
			{
				context.write(new IntWritable(temperature),new Text(record.toString()));
			}
			
		}
	}

	public int run(String[] args) throws Exception {
		if(args.length!=2)
    	{
    		System.out.println("Insufficient number of arguments passed");
    		System.exit(1);
    	}
    	String input = args[0];
    	String output = args[1];
    	Configuration jobConfig = new Configuration();    	
        Job job = new Job(jobConfig,"sortData");
        job.setJarByClass(SortProgram.class);
        job.setJobName("sortProgram");
        job.setMapperClass(SortMapper.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(2);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(MapFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        return job.waitForCompletion(true) ? 0 : 1;
	
	}
	
	public static void main(String args[]) throws Exception
	{
		int status = ToolRunner.run(new SortProgram(), args);
		if(status != 0)
		{
			System.exit(1);
		}
	}

}
