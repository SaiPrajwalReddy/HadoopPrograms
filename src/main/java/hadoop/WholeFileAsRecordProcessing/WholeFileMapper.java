package hadoop.WholeFileAsRecordProcessing;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

	private static final Log LOG = LogFactory.getLog(WholeFileMapper.class);

	Text fileName;

	public void setUp(Context context) {
		InputSplit split = context.getInputSplit();
		Path pt = ((FileSplit) split).getPath();
		fileName = new Text(pt.toString());

	}

	public void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
		
		LOG.info("Filename is "+ fileName);
		System.out.println(fileName);
		context.write(new Text(key.toString()), value);
	}

}
