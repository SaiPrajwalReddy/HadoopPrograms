package hadoop.WholeFileAsRecordProcessing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WholeFileMain extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		
		if(args.length!=2)
    	{
    		System.out.println("Insufficient number of arguments passed");
    		System.exit(1);
    	}
    	String input = args[0];
    	String output = args[1];
    	Configuration c = new Configuration();    	
		// TODO Auto-generated method stub
		Job job = new Job(c,"processWholeFileAsRec");
		job.setJarByClass(WholeFileMain.class);
		job.setJobName("processWholeFileAsRec");
		job.setMapperClass(WholeFileMapper.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setInputFormatClass(WholeFileInpuFormattReader.class);	
		FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String args[]) throws Exception
	{
		ToolRunner.run(new WholeFileMain(), args);
	}

}
