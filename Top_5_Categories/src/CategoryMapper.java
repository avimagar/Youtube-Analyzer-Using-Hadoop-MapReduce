import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;




public class CategoryMapper extends MapReduceBase implements Mapper
<LongWritable,Text,Text,IntWritable>{

	
	Text category = new Text();
	IntWritable one = new IntWritable(1);
	
	
	public void map(LongWritable arg0, Text value,OutputCollector<Text, IntWritable> output,
			Reporter report) throws IOException {
	
		String line = value.toString();
        String str[]=line.split("\t");

       if(str.length > 5){
    	   category.set(str[3]);
      		   
       }

       output.collect(category, one);
    	
	}
	
}
