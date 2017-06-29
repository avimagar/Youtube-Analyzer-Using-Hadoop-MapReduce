import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*
 * 
 * Driver Class For finding top 10 videos which are having highest number of views.
 * 
 */
public class Top_10_Views extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Provide input and output directories ");
			System.exit(-1);
		}

		Job job = Job.getInstance(getConf());

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(Views_Mapper.class);
		job.setReducerClass(Views_Reducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		FileSystem fs=FileSystem.get(getConf());

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		return job.waitForCompletion(true) ? 0: 1;
	}

	public static void main(String[] args) throws Exception {
		Top_10_Views driver = new Top_10_Views();	
		int res = ToolRunner.run(driver, args);
		System.exit(res);
	}
}