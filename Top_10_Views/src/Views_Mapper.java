import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
*	Input (Key,Value)=(LongWritable,Text)
*	Treemap (Key,Value)=(Views,VideoId)
*	Output (Key,Value)=(VideoId,Views)
*
*/

//Comparator class for TreeMap data strucure.
class Comp implements Comparator<Long>{

    public int compare(Long object1, Long object2)  {

        if(object2>object1)
            return 1;
        else
            return -1;
    }
    
}



public class Views_Mapper extends Mapper<LongWritable,Text,Text,LongWritable>
{

	static TreeMap <Long,String> localOutput=new TreeMap<Long,String>(new Comp());
	static Text videoID=new Text();
	static LongWritable views=new LongWritable();
	

	@Override
	public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
		
		String line=inputValue.toString();
		String tmp_str[]=line.split("\\t");
		if(tmp_str.length>6)
		{
			if(tmp_str[5].matches("\\d+")){
				Long tmpViews=Long.parseLong(tmp_str[5]);
				localOutput.put(tmpViews, tmp_str[0]);
			}
			
			//keep only 10 elements in the local treemap by removing last elements
			if(localOutput.size()>10)
			{
				localOutput.remove(localOutput.lastKey());         
			}
		
		}
		
		
	}
	
	//Called once at the end of the task
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		 
		Set <Entry<Long,String>> entryset=localOutput.entrySet();
		Iterator <Entry<Long,String>>itr = entryset.iterator();
		Entry<Long, String> entry=null;
		for(int i=0;i<10;i++)
		{
			entry=itr.next();
			videoID.set((String) entry.getValue());
			views.set((Long) entry.getKey());
			context.write(videoID, views);
		}
	}
	

}
