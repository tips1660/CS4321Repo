package tree;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import project2DB.*;

public class TreeDeserializer {
	public ByteBuffer buffer;
	public FileInputStream fs;
	public FileChannel fc;
	public File indexFile;
	
	//Header Page Data
	public int rootAddress;
	public int numLeaves;
	public int order;
	
	//Keep track of where we are in the Index file
	public LeafNode currLeaf;
	public int currLeafNodeAddr;
	
	
	//Keep track of where we are as we go through the leaf node
	private int currKey;
	private Enumeration<Integer> keys;
	private int currRId;
	
	private Integer lowkey;
	private Integer highkey;
	
	
	/*
	 * Constructor for a treeDeserializer object on a given index file. 
	 */
	public TreeDeserializer(File indexFile, Integer lowkey, Integer highkey) throws IOException{
		
		this.indexFile = indexFile;
		this.lowkey = lowkey;
		this.highkey = highkey;
		
		buffer = ByteBuffer.allocate(4096);
		fs = new FileInputStream(indexFile);
		fc = fs.getChannel();
		extractHeaderData();
		currLeaf = findLowKey(lowkey);
		currLeafNodeAddr = currLeaf.pageNumber;
		currKey= 0;
		currRId = 0;
			
	}
	
	
	/*
	Navigate root-to-leaf to find lowkey (or where lowkey would be if it were in the tree, 
	since it may not be present) and grab the next data entry from the leaf.
	*/
	
	private LeafNode findLowKey(Integer lowkey) throws IOException{
		
		//no lower bound, return very first leaf
		if (lowkey == null){
			return (LeafNode) deserializePage(1);
			
		}
		else{

			IndexNode root = (IndexNode) deserializePage(rootAddress);
			TreeNode curr = root;
			int pageAddr = 1;
			
			//Keep traversing down index nodes until we find a leaf
			while (curr instanceof IndexNode){
				IndexNode currIndex = (IndexNode) curr;
				
				/**
			 	From Collections.binarySearch:
				
				Returns the index of the search key, if it is contained in the list; otherwise, (-(insertion point) - 1). 
				The insertion point is defined as the point at which the key would be inserted into the list: 
				the index of the first element greater than the key, or list.size() if all elements 
				in the list are less than the specified key. 
				Note that this guarantees that the return 
				value will be >= 0 if and only if the key is found.
				*/
				
				int page = Collections.binarySearch(currIndex.getKeys(),lowkey+1);
				page = (page >= 0) ? page : -(page+1);
				
				//Using index we found in the keylist, get the address of the next page to traverse to
				pageAddr = currIndex.getKeys().get(page);
				
				curr = deserializePage(pageAddr);
				
			}
			//Reached leafnode
			return (LeafNode) curr;

		}
		
	}
	
	/**
	 * 
	 * @param i the page number
	 * @return TreeNode representation of the deserialized data on page i
	 * @throws IOException 
	 */
	private TreeNode deserializePage(Integer i) throws IOException{
		//Load page into buffer
		getPage(i);
		int nodeType = buffer.get(0);
		return (nodeType == 1) ? deserializeIndex(i) : deserializeLeaf(i);
	
	}

	/**
	 * @param pageNum The page number of the Node
	 * @return Deserialized IndexNode representation of all the data on that page 
	 * 
	 * Note that this representation does not have children as an ArrayList of TreeNodes, 
	 * but as an ArrayList of Integer pageIDs for the IndexScanOperator to use
	 */
	private IndexNode deserializeIndex(int pageNum){
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
		n.setChildPages(children);
		n.pageNumber = pageNum;
		return n;
		
		
	}
	
	/**
	 * Deserialize a page into LeafNode
	 * Precondition: Correct page is already loaded into the buffer.
	 * @return LeafNode representation of the current page
	 */
	private LeafNode deserializeLeaf(int pageNum){
		
		int pointer = 4;
		int numdata = buffer.getInt(pointer);
		pointer+=4;
		int processed = 0;
		
		Hashtable<Integer,ArrayList<RId>> dataEntries = new Hashtable<Integer,ArrayList<RId>>();
		
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
				rids.add(new RId(p,t));
				processed_rids++;
			}
			dataEntries.put(key,rids);
			processed++;	
		}
		
		LeafNode leaf = new LeafNode(dataEntries, pageNum);
		return leaf;
	
	}
	
	
	//should only be called once in the constructor.
	//Extracts header information from the serialized index file.
	//Clear buffer afterwards
	private void extractHeaderData() throws IOException {
		buffer.clear();
		fc.read(buffer);
		rootAddress = buffer.getInt(0);
		numLeaves = buffer.getInt(4);
		order = buffer.getInt(8); 
		buffer.clear();
		buffer.put(new byte[4096]);
		buffer.clear();

	}
	
	/**
	 * 
	 * @param pageID
	 * @throws Loads page PageID of the index file into the buffer
	 */
	private void getPage(int pageID) throws IOException{
		buffer.clear();
		buffer.put(new byte[4096]);
		buffer.clear();
		
		long index = 4096 * pageID;
		fc.position(index);
		fc.read(buffer);
		buffer.flip();
		
	}
	
	/**
	 * Sets global currLeaf to the next leaf node, and
	 * updates the keys enum and currkey to the first key in the enum.
	 * 
	 * @throws IOException
	 */
	public void moveToNextLeaf() throws IOException{
		currLeafNodeAddr++;
		currRId = 0;
		
		TreeNode temp = deserializePage(currLeafNodeAddr);
		if (temp instanceof LeafNode){
			currLeaf = (LeafNode) temp;
			keys = currLeaf.getDataEntry().keys();
			currKey = keys.nextElement();
		}
		else{
			//Reached last leaf node. no more
			currLeaf = null;
		}
	}

	
	/**
	 * 
	 * @return the next RID in the scan. null if no more RIds to scan
	 * @throws IOException 
	 */
	public RId getNextRId() throws IOException {
		
		
		//First call, initialize keys.
		//Find the first key that is greater than or equal to lowkey
		if (keys == null){
			keys = currLeaf.getDataEntry().keys();
			currKey = keys.nextElement();
			
			while (currLeaf != null && currKey < lowkey){
				if (!keys.hasMoreElements()) moveToNextLeaf();
				else currKey = keys.nextElement();
			}
			
			if (currLeaf == null) return null;
			else return currLeaf.getDataEntry().get(currKey).get(currRId++);
			
		}
		
		//Reached end of the RIds list, must move on to the next key if present
		if (currRId >= currLeaf.getDataEntry().get(currKey).size()){
			
			//Check if finished looking at all the keys of the page, move onto next page
			if (keys.hasMoreElements()) currKey = keys.nextElement();
			else moveToNextLeaf();
			if (currLeaf == null) return null;
		}
		
		//If highkey is null return until we reach the end of the leaf nodes.
		//Else we return RIDs up until 
		if (highkey == null) return currLeaf.getDataEntry().get(currKey).get(currRId++);
		else return (currKey < highkey) ? currLeaf.getDataEntry().get(currKey).get(currRId++) : null;
		
		
	}
	
	

}
