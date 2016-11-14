package project2DB;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class TupleReader {

	private String fName = "";
	private FileInputStream fin;
    private FileChannel fc;
    private ByteBuffer buffer = ByteBuffer.allocate(4096);
    private int numAttributes;
    private int numTuples;
    private int currentTuple;
    private int currentByte;
    private ArrayList<Integer> tuple;
    private int currentPage = -1;
    private int tupleNum = -1;
  
    //Constructor for index scan to handle lowkey
    public TupleReader(String fileName, Integer pageID, Integer tupleID) throws IOException{
    	tuple = new ArrayList<Integer>();
    	fName = fileName;
    	fin = new FileInputStream(fName);
    	fc = fin.getChannel();
    	
    	//set the scan to begin reading at the given pid, tid
    	buffer.clear();
		buffer.put(new byte[4096]);
		buffer.clear();
		
		long index = 4096 * pageID;
		fc.position(index);
		getNextPage();
		
		//potential off by one error based on indexing
		while (currentTuple < tupleID-1){
			tuple = getNextTuple();
		}
		
    }
    
    public TupleReader(String fileName) throws IOException {
    	tuple = new ArrayList<Integer>();
    	fName = fileName;
    	System.out.println(fName.toString());
    	fin = new FileInputStream(fName);
    	fc = fin.getChannel();
    	getNextPage();
    }
    
    public TupleReader() throws IOException {
    	
    }
    
    public void changeFile(String fileName) throws IOException {
    	tuple = new ArrayList<Integer>();
    	fName = fileName;
    	//System.out.println(fName.toString());
    	fin = new FileInputStream(fName);
    	fc = fin.getChannel();
    	getNextPage();
    }
    
    public int getNextPage() throws IOException {
    	currentPage+=1;
    	buffer.clear();
    	int numBytes = fc.read(buffer);
    	//System.out.println("getNextPage was called");
    	if (numBytes != -1)
    		setInfo();
    	return numBytes;
    }
    
    public void setInfo() {
    	numAttributes = buffer.getInt(0);
    	//System.out.println(numAttributes);
    	numTuples = buffer.getInt(4);
    	//System.out.println(numTuples + " setInfo called");
    	currentTuple = 0;
    	currentByte = 8;
    }
    
    public int getNumAttributes() {
    	return numAttributes;
    }
    
    public int getNumTuples() {
    	return numTuples;
    }
    
    public ArrayList<Integer> getArrayList() {
    	return tuple;
    }
    
    public ArrayList<Integer> getNextTuple() throws IOException {
    	if (!tuple.isEmpty())
    		tuple.clear();
    	int numBytes = 0;
    	if (!(currentTuple < numTuples)) {
    		//buffer.clear();
    		numBytes = getNextPage();
    	}
    	if (numBytes != -1) {
    		int currentAttribute = 0;
    		tupleNum++;
    		while (currentAttribute < numAttributes) {
    			currentAttribute++;
    			//System.out.print(buffer.getInt(currentByte) + " ");
    			tuple.add(buffer.getInt(currentByte));
    			currentByte += 4;
    		}
    		//System.out.println();
    	}
    	currentTuple++;
    	return tuple;
    }
    public int getTupleNum()
    {
    	return tupleNum;
    }
    public int getPage()
    {
    	return currentPage;
    }
}
