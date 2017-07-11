import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
*	Input (Key,Value)=(VideoId,Views)
*	Treemap (Key,Value)=(Views,VideoId)
*	Output (Key,Value)=(VideoId,Views)
*/


public class Views_Reducer extends Reducer <Text,LongWritable,Text,LongWritable> {

	static TreeMap <Long,String> finalOutput=new TreeMap<Long,String>(new Comp());

	//Following method is called once for each key. 
	@Override
	public void reduce(Text inputKey, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
	
		Long totalViews=(long) 0;
		for(LongWritable value:values )
		{
			long temp=value.get();
			if(totalViews < temp)
				totalViews=temp;
			
		}
		finalOutput.put(totalViews,inputKey.toString());
	
		//keep only 10 elements in the treemap by removing last elements 
		if(finalOutput.size()>10)
		{
			finalOutput.remove(finalOutput.lastKey());
		}
	
	}
	
	//Called at the end of reducer
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		 
		Set <Entry<Long,String>> entryset=finalOutput.entrySet();
		Iterator <Entry<Long,String>>itr=entryset.iterator();
		Entry<Long, String> entry=null;
		Text videoId=new Text();;
		LongWritable views=new LongWritable();
		for(int i=0;i<10;i++)
		{
			entry=itr.next();
			videoId.set((String) entry.getValue());
			views.set((Long) entry.getKey());
			context.write(videoId,views);
		}
	
	}
}
