package hadoop.WholeFileAsRecordProcessing;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WholeFileInpuFormattReader extends FileInputFormat<NullWritable, BytesWritable> {

	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	@Override
	public org.apache.hadoop.mapreduce.RecordReader<NullWritable, BytesWritable> createRecordReader(
			org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		WholeFileReader wfr = new WholeFileReader();
		wfr.initialize(split, context);
		return wfr;
	}

}
