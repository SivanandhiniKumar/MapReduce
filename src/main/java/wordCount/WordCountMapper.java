package wordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>
{
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException 
	{
		System.out.println("Cleanup called");
		super.cleanup(context);
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException
	{
		System.out.println("Setup called");
		super.setup(context);
	}

	@Override
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
	{
		System.out.println("Mapper called");
		String line = value.toString();
		String[] words=line.split(",");
	
		for(String word: words )
		{
		      Text outputKey = new Text(word.toUpperCase().trim());
			  IntWritable outputValue = new IntWritable(1);
			  con.write(outputKey, outputValue);
		}

	}
}
