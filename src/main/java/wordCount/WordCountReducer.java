package wordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{

	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException
	{
		System.out.println("Cleanup called");
		super.cleanup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context con) throws IOException, InterruptedException
	{
		System.out.println("Reducer Called");
		int sum = 0;

		   for(IntWritable value : values)
		   {
			   sum += value.get();
		   }
		   con.write(key, new IntWritable(sum));
	}

	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException
	{
		System.out.println("Setup called");
		super.setup(context);
	}
	
}
