package project2DB;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;

public class TupleWriter {

	private String fname = "";
	FileOutputStream fout;
	FileChannel fc;
	private ByteBuffer buffer = ByteBuffer.allocate(4096);
	private int numAttributes;
	private int numTuples;
	private int currentByte;
	private int startOfTuple;
	private Tuple tuple;
	private ArrayList<Integer> values = new ArrayList<Integer>();
	
	public TupleWriter(String fname) throws IOException {
		System.out.println("TupleWriter was initialized");
		System.out.println(fname.toString());
		fout = new FileOutputStream(fname);
		fc = fout.getChannel();
		currentByte = 0;
	}
	
	public void changeFile(String fname) throws IOException{
		fout = new FileOutputStream(fname);
		fout.getChannel();
		currentByte = 0;
	}
	
	public Tuple getTuple() {
		return tuple;
	}
	
	public int getNumAttributes() {
		return numAttributes;
	}
	
	public int getNumTuples() {
		return numTuples;
	}
	 
	public ArrayList<Integer> tupleSort(Tuple t)
	{
		ArrayList<Integer> val = new ArrayList<Integer>();
		ArrayList<String> items = t.getItemList();
		//System.out.println(t.getValues());
		for(int i =0; i< items.size(); i++)
		{
			val.add((Integer)t.getValues().get(items.get(i)));
		}
		return val;
	}
	public void writeTuple(Tuple t) throws IOException {
		System.out.println("writeTuple method in tuple writer was called");
		tuple = t; 
		if (tuple == null) {
			System.out.println("Writer got null tuple. Returning.");
			return;
		}

		if (tuple.getTable().equals("ENDOFFILE")) {
			System.out.println("Writer got end of file. Returning");
			return;
		}
		
		values = tupleSort(t);

		System.out.println(values);
		if (currentByte == 0) {
			System.out.println("initializing a new page");
			numAttributes = values.size();
			numTuples = 0;
			buffer.putInt(currentByte, numAttributes);
			currentByte = 8;
		}
		numTuples++;
		System.out.println("The number of tuples so far on this page is "+numTuples);
		System.out.println("The number of attributes is "+numAttributes);
		startOfTuple = currentByte;
		boolean wasLast = false;
		for (int i = 0; i < values.size(); i++) {
			
			buffer.putInt(4, numTuples);
			if (currentByte < 4096) {
				buffer.putInt(currentByte, values.get(i));
				System.out.println("The value at :" + currentByte+" is " +buffer.getInt(currentByte));
				currentByte += 4; 
				if (currentByte == 4096 && i == (values.size()-1)) {
					wasLast = true;
				}
			}
			else {
				break;
			}
				
		}
		if (currentByte >= 4096 && !wasLast) {
			currentByte = startOfTuple;
			numTuples--;
			buffer.putInt(4, numTuples);
			while (currentByte < 4096) {
				buffer.putInt(currentByte, 0);
				System.out.println("The value at :" + currentByte+" is " +buffer.getInt(currentByte));
				currentByte += 4;
			}
			write();
			writeTuple(t);
		}
	}
	
	public void write() throws IOException {
		System.out.println("write method in TupleWriter was called");
		while (currentByte < 4096) {
			buffer.putInt(currentByte, 0);
			currentByte += 4;
		}
		//buffer.flip();
		fc.write(buffer);
		buffer.clear();
		currentByte = 0;
	}
}
