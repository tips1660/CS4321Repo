package tree;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.*;
import java.nio.channels.FileChannel;

public class TreeDeserializer {
	public ByteBuffer buffer;
	public FileInputStream fs;
	public FileChannel fc;
	public File indexFile;
	public int rootAddress;
	public int numLeaves;
	public int order;
	
	private LeafNode currLeaf;
	
	TreeDeserializer(File indexFile, Integer lowkey, Integer highkey) throws IOException{
		
		this.indexFile = indexFile;
		buffer = ByteBuffer.allocate(4096);
		fs = new FileInputStream(indexFile);
		fc = fs.getChannel();
		extractHeaderData();
			
	}

	private void findLowKey(Integer lowkey){
		
		//no lower bound
		if (lowkey == null){
			currLeaf = deserializeNode(1);
			
		}
		else{
			
			
		}
		
	}
	
	private void deserializePage(Integer i){
		getPage(i);
		int nodeType = buffer.get(0);
		
		
	}
	
	//should only be called once in the constructor.
	//Extracts header information from the serialized index file.
	private void extractHeaderData() throws IOException {
		buffer.clear();
		int header = fc.read(buffer);
		rootAddress = buffer.getInt(0);
		numLeaves = buffer.getInt(4);
		order = buffer.getInt(8); 

	}
	
	//Loads the given pageID into the buffer
	private void getPage(int pageID) throws IOException{
		buffer.clear();
		buffer.put(new byte[4096]);
		buffer.clear();
		
		long index = 4096 * pageID;
		fc.position(index);
		fc.read(buffer);
		buffer.flip();
		
	}
	
	

}
