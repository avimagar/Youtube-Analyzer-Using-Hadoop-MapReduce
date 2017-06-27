import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class CategoryReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> 
{

	public void reduce(Text inputkey, Iterator<IntWritable> inputvalues,OutputCollector<Text, IntWritable> output, Reporter reporter)throws IOException 
	{
		int sum = 0;
        	while(inputvalues.hasNext()) 
		{
        	
	        	IntWritable i=inputvalues.next();
		        sum += i.get();
            
        	}
		output.collect(inputkey,new IntWritable(sum));

	}

}
