package wordCountWithFileName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FileNameCount implements Writable{
	String filename;
	int count;
	
	public FileNameCount() {
		filename = "";
		count = 0;
	}

	public FileNameCount(String file, Integer value) {
		filename = file;
		count = value;
	}
	
	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		filename = in.readUTF();
		count = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(filename);
		out.writeInt(count);
		
	}

	@Override
	public String toString() {
		String s = filename + " " +count;
		return s;
	}

	
	
}
