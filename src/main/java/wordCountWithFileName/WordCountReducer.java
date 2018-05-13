package wordCountWithFileName;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, FileNameCount, Text, FileNameCount>
{

	@Override
	protected void cleanup(Reducer<Text, FileNameCount, Text, FileNameCount>.Context context)
			throws IOException, InterruptedException
	{
		System.out.println("Cleanup called");
		super.cleanup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<FileNameCount> values,
			Reducer<Text, FileNameCount, Text, FileNameCount>.Context con) throws IOException, InterruptedException
	{
		System.out.println("Reducer Called");
		Map<String,Integer> map = new HashMap<String,Integer>();
		

	   for(FileNameCount value : values)
	   {
		   if(!map.containsKey(value.getFilename()))
		   {
			   map.put(value.getFilename(), 1);
		   }
		   else
		   {
			   int count = map.get(value.getFilename());
			   map.put(value.getFilename(), ++count);
		   }
	   }
	   for (Entry<String, Integer> entry : map.entrySet())
	   {
		   con.write(key, new FileNameCount(entry.getKey(), entry.getValue()));
	   }
	}

	@Override
	protected void setup(Reducer<Text, FileNameCount, Text, FileNameCount>.Context context)
			throws IOException, InterruptedException
	{
		System.out.println("Setup called");
		super.setup(context);
	}
	
}
