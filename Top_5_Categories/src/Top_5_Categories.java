import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Top_5_Categories extends Configured implements Tool{

	
	public int run (String []arr) throws IOException
	{
		
		//Check for appropriate input command line arguments
		if(arr.length<2)
		{
			System.out.println("Plz give appropriate directories");
			return -1;
		}

		JobConf conf=new JobConf(Top_5_Categories.class);
		

		FileInputFormat.setInputPaths(conf, new Path(arr[0]));
		FileSystem.get(conf).delete(new Path(arr[1]),true);
		FileOutputFormat.setOutputPath(conf, new Path(arr[1]));
		
		
		//Setting Mapper & reducer classes
		conf.setMapperClass(CategoryMapper.class);
		conf.setReducerClass(CategoryReducer.class);
	    
		//Setting Mapper's output type
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		//Setting Reducer's output type
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		

		JobClient.runJob(conf);
		return 0;
		
	}

	
	
	public static void main(String [] args) throws Exception{
	
		int exitcode=ToolRunner.run(new Top_5_Categories(), args);
		System.exit(exitcode);
		
	}
}
