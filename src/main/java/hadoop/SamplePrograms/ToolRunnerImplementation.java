package hadoop.SamplePrograms;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ToolRunnerImplementation extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration c = getConf();
		for(Entry<String, String> val : c)
		{
			System.out.println("Key :" + val.getKey()+"Value :"+val.getValue());
		}
		return 0;
	}
	
	public static void main(String args[]) throws Exception
	{
		ToolRunner.run(new ToolRunnerImplementation(), args);
	}

}
