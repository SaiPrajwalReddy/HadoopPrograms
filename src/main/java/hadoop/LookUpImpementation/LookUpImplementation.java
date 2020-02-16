package hadoop.LookUpImpementation;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LookUpImplementation extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("Insufficient number of arguments passed");
			System.exit(1);
		}

		Path pt = new Path(args[0]);
		IntWritable key = new IntWritable(Integer.parseInt(args[1]));
		Reader[] readers = MapFileOutputFormat.getReaders(pt, getConf());
		Partitioner<IntWritable, Text> part = new HashPartitioner<IntWritable, Text>();
		Text value = new Text();
		Reader reader = readers[part.getPartition(key, value, readers.length)];
		Writable entry = reader.get(key, value);
		if (entry == null) {
			return -1;
		}
		
		IntWritable nextKey = new IntWritable();
		do {
			int temperature = 0;
			String year = "";
			String record = value.toString();			
			year = record.substring(15, 19);
			if (record.charAt(87) == '+' || record.charAt(87) == '-') {
				temperature = Integer.parseInt(record.substring(88, 92));
			} else {
				temperature = Integer.parseInt(record.substring(87, 92));
			}
			System.out.printf("%s %s", year,temperature);
		}while(reader.next(nextKey, value) && key.equals(nextKey));
		// TODO Auto-generated method stub
		return 0;
	}

	public static void main(String args[]) throws Exception {
		int status = ToolRunner.run(new LookUpImplementation(), args);
		System.exit(status);
	}
}
