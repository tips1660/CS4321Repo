package tree;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TreeDeserializer {
	public ByteBuffer buffer;
	public FileInputStream fs;
	public FileChannel fc;
	public File indexFile;
	
	public int rootAddress;
	public int numLeaves;
	public int order;
	
	
	
	
	//private TreeNode currLeaf;
	
	public TreeDeserializer(File indexFile, Integer lowkey, Integer highkey) throws IOException{
		
		this.indexFile = indexFile;
		buffer = ByteBuffer.allocate(4096);
		fs = new FileInputStream(indexFile);
		fc = fs.getChannel();
		extractHeaderData();
		findLowKey(lowkey);
			
	}

	//
	private void findLowKey(Integer lowkey) throws IOException{
		
		//no lower bound
		if (lowkey == null){
			deserializePage(1);
			
		}
		else{
			//We must traverse to the first leafnode with lowkey value
			//deserializePage(rootAddress);
			//traverse through index nodes until we are at leaf
			//
			
			deserializePage(rootAddress);
			IndexNode root = deserializePage(rootAddress);
			TreeNode curr = root;
			pageAddr = 1;
			/*
			while (curr instanceof IndexNode){
				IndexNode currIndex = (IndexNode) curr;
				int page = Collections.binarySearch(currIndex,lowkey+1);
				
			}
			
			 * while curr is an Index Node
			 * 		deserialize it
			 * 		Binary search through keys of index to find which path to traverse
			 * 		Follow that pointer to the next node
			 * 		update pageAddr to current page
			 * 		Stop if we reach a leaf node
			 * deserialize that final leaf node and return it
			 */
			
			
			
		}
		
	}
	
	
	private TreeNode deserializePage(Integer i) throws IOException{
		getPage(i);
		
		//0 leaf 1 index
		int nodeType = buffer.get(0);
		
		if (nodeType == 1) return deserializeIndex();
		else return deserializeLeaf();
	
	}

	private IndexNode deserializeIndex(){
		int numkeys = buffer.getInt(4);
		int pointer = 8;
		
		ArrayList<Integer> keys = new ArrayList<Integer>();
		ArrayList<Integer> children = new ArrayList<Integer>();
		
		//obtain all keys
		while (pointer < 8 + numkeys*4){
			keys.add(buffer.getInt(pointer));
			pointer+=4;
		}
		
		//obtain all children. Should be numkeys+1.
		while (pointer < 8 + numkeys*4 + (numkeys+1)*4){
			children.add(buffer.getInt(pointer));
			pointer+=4;
		}
		
		IndexNode n = new IndexNode();
		n.setKeys(keys);
		
		//children?
		
	}
	
	/**
	 * Deserialize a leaf node into a Map of leaf keys to p->t pairs
	 * p-t pairs for each key are represented as a HashMap from pi to ti
	 * Precondition: Correct page is already loaded into the buffer.
	 * @return Data Structure containing deserialized data entries of a leaf page in the buffer.
	 */
	private LeafNode deserializeLeaf(){
		
		int pointer = 4;
		int numdata = buffer.getInt(pointer);
		pointer+=4;
		int processed = 0;
		
		Hashtable<Integer,List<RId>> dataEntries = new Hashtable<Integer,List<RId>>();
		
		while (processed < numdata){
			int key = buffer.getInt(pointer);
			pointer+=4;
			int num_rids = buffer.getInt(pointer);
			pointer+=4;
			int processed_rids = 0;
			
			ArrayList<RId> rids = new ArrayList<RId>();
			
			while (processed_rids < num_rids){
				int p = buffer.getInt(pointer);
				pointer+=4;
				int t = buffer.getInt(pointer);
				pointer+=4;
				pairs.put(new RId(p,t));
				processed_rids++;
			}
			dataEntries.put(key,rids);
			processed++;	
		}
		return dataEntries;
	
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
