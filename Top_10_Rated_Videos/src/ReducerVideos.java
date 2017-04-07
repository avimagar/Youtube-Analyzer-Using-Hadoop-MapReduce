import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;







public class ReducerVideos extends MapReduceBase implements Reducer
<Text,FloatWritable,Text,FloatWritable>{

	public void reduce(Text key, Iterator<FloatWritable> values,
			OutputCollector<Text, FloatWritable> output, Reporter report)
			throws IOException {
	
		 float sum=0;
		 while(values.hasNext())
		 {
			 FloatWritable tmp=values.next();
			 sum+=tmp.get();
			 
		 }
		 output.collect(key,new FloatWritable(sum));
	}

}
