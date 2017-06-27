import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;




public class MapperVideos extends MapReduceBase implements Mapper<LongWritable,Text,Text,FloatWritable>
{

	public void map(LongWritable inputkey, Text inputvalue, OutputCollector<Text, FloatWritable> output,
			Reporter reporter) throws IOException 
		{
			Text videoname=new Text();
			FloatWritable rating=new FloatWritable();
	
			String line = inputvalue.toString();
			String tempstr[]=line.split("\t");
	    
			if(tempstr.length>6) 
			{  
				videoname.set(tempstr[0]);
				if(tempstr[6].matches("\\d+.\\d+"))
				{ 
					float f=Float.parseFloat(tempstr[6]); 
					rating.set(f);
	
				}
			}
		  
		 output.collect(videoname,rating);
	}
}
