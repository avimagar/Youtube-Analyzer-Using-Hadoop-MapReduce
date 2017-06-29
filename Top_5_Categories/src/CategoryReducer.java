import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CategoryReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
{

	public void reduce(Text inputkey, Iterator<IntWritable> inputvalues,Context context)throws IOException,InterruptedException 
	{
		int sum = 0;
        	while(inputvalues.hasNext()) 
		{
        	
	        	IntWritable i=inputvalues.next();
		        sum += i.get();
            
        	}
		context.write(inputkey,new IntWritable(sum));

	}

}
