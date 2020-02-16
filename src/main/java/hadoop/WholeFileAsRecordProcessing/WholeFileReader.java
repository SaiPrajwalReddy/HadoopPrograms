package hadoop.WholeFileAsRecordProcessing;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

public class WholeFileReader extends RecordReader<NullWritable, BytesWritable> {

	FileSplit fileSplit;
	Configuration conf;
	boolean processed = false;
	BytesWritable b = new BytesWritable();

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (!processed) {
			byte[] contents = new byte[(int) fileSplit.getLength()];
			Path pt = fileSplit.getPath();
			FileSystem fs = null;
			fs = FileSystem.get(conf);
			FSDataInputStream fsin = fs.open(pt);
			try {
				IOUtils.readFully(fsin, contents, 0, contents.length);
				b.set(contents, 0, contents.length);
			} finally {
				IOUtils.closeQuietly(fsin);
			}			
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return NullWritable.get();		
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return b;		
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return processed?1:0;		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

}
