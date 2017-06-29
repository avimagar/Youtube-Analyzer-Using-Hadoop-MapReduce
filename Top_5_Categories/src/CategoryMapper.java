import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class CategoryMapper extends Mapper <LongWritable,Text,Text,IntWritable>{

	
	Text category = new Text();
	IntWritable one = new IntWritable(1);
	
	
	public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException 
	{
		
		String line = inputValue.toString();
        	String tempstr[]=line.split("\t");

       		if(tempstr.length > 5)
		{
    	   		category.set(tempstr[3]);
    
       		}

       		context.write(category, one);
    	
       }
	
}
