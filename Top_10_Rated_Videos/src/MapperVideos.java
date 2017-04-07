import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;




public class MapperVideos extends MapReduceBase implements Mapper
<LongWritable,Text,Text,FloatWritable>{

	

	public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output,
			Reporter arg3) throws IOException {
		Text video_name=new Text();
		FloatWritable rating=new FloatWritable();
	
		  String line = value.toString();
		  String str[]=line.split("\t");
	    
		  if(str.length>6) 
		  {  
			  video_name.set(str[0]);
		      if(str[6].matches("\\d+.+"))
		      { 
		    	  float f=Float.parseFloat(str[6]); 
		          rating.set(f);
	
		      }
		  }
		  output.collect(video_name,rating);
	}
}