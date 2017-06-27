import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;







public class Top_10_Rated_Videos extends Configured implements Tool{

	public int run(String[] arr) throws Exception {

		if(arr.length<2)
		{
			System.out.println("Please give appropriate directories");
			return -1;
	
			
		}

		JobConf conf=new JobConf(Top_10_Rated_Videos.class);
		

		FileInputFormat.setInputPaths(conf, new Path(arr[0]));
		FileSystem.get(conf).delete(new Path(arr[1]),true);
		FileOutputFormat.setOutputPath(conf, new Path(arr[1]));
				
		
		conf.setMapperClass(MapperVideos.class);
		conf.setReducerClass(ReducerVideos.class);
		
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(FloatWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(FloatWritable.class);
		
		JobClient.runJob(conf);
		return 0;
		
		}

		public static void main(String [] args) throws Exception{
			
			int EXITCODE=ToolRunner.run(new Top_10_Rated_Videos(), args);
			System.exit(EXITCODE);
			
		}
	
	
	
}
