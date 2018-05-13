package wordCountWithFileName;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,FileNameCount>
{
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, FileNameCount>.Context context)
			throws IOException, InterruptedException 
	{
		System.out.println("Cleanup called");
		super.cleanup(context);
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, FileNameCount>.Context context)
			throws IOException, InterruptedException
	{
		System.out.println("Setup called");
		super.setup(context);
	}

	@Override
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
	{
		String fileName = ((FileSplit) con.getInputSplit()).getPath().getName();
		System.out.println("Mapper called");
		String line = value.toString();
		String[] words=line.split(",");
		for(String word: words )
		{
		      Text outputKey = new Text(word.toUpperCase().trim());
			  //IntWritable outputValue = new IntWritable(1);
		      //Text outputValue = new Text(fileName);
			  con.write(outputKey, new FileNameCount(fileName, 1));
		}

	}
}
