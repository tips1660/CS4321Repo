package project2DB;
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
	public LeafNode finalLeaf;
	public int currLeafNodeAddr;
	private ArrayList<LeafNode> leaves;
	private ArrayList<RId> ridlist;
	public int bufferCtr =0;
	
	
	//Keep track of where we are as we go through the leaf node
	private int currKey;
	private Enumeration<Integer> keys;
	private int currRId;
	
	private Integer lowkey;
	private Integer highkey;
	
	
	/**
	 * The constructor also finds the LeafPage where the lowkey value is present (if lowkey exists)
	 * or defaults to leaf page 1. It sets the global currLeaf to the LeafNode where lowkey is present,
	 * and saves the pageAddr of this LeafNode into currLeafNodeaddr. 
	 * 
	 * @param indexFile the index file to be deserialized
	 * @param lowkey the lower scan boundary, null if not needed
	 * @param highkey the upper scan boundary, null if not needed
	 * @throws IOException
	 */
	public TreeDeserializer(File indexFile, Integer lowkey, Integer highkey) throws IOException{
	
		this.indexFile = indexFile;
		this.lowkey = lowkey;
		this.highkey = highkey;
		leaves = new ArrayList<LeafNode>();
		
		buffer = ByteBuffer.allocate(4096);
		fs = new FileInputStream(indexFile);
		fc = fs.getChannel();
		extractHeaderData();
		currLeaf = findKey(lowkey, true);
		finalLeaf = findKey(highkey, true);
		currLeafNodeAddr = currLeaf.pageNumber;
		currKey= 0;
		currRId = 0;
		extractNodes();
			
	}
	public void extractRIDs()
	{
		for(int i =0; i< leaves.size(); i++)
		{
			Object[] arr = leaves.get(i).getDataEntry().keySet().toArray();
			for(int j =0; j< arr.length; j++)
			{
				int keyVal = (int)arr[j];
				if(keyVal>= lowkey && keyVal <= highkey)
				{
					ArrayList<RId> myRId = leaves.get(i).getDataEntry().get(keyVal);
					for(int p =0; p < myRId.size(); p++)
					{
						ridlist.add(myRId.get(p));
					}
				}
			}
		}
	}
	/*
	 * get all the nodes in between my low key node and my high key node.
	 */
	public void extractNodes() throws IOException
	{   leaves.add(currLeaf);
		int currentPage = currLeaf.getAddress();
		while(currentPage < finalLeaf.getAddress())
		{
			LeafNode l = (LeafNode)deserializePage(currentPage);
			leaves.add(l);
			currentPage++;
		}
		leaves.add(finalLeaf);
	}
	
	public ArrayList<LeafNode> getLeaves()
	{
		return leaves;
	}
	/**
	 * Navigate root-to-leaf to find lowkey (or where lowkey would be if it were in the tree, 
	 *	since it may not be present) and grab the next data entry from the leaf.
	 * @param lowkey the lowkey boundary, null if not needed
	 * @return LeafNode that contains the lowkey, or contains the range where lowkey falls between.
	 * @throws IOException
	 */
	private LeafNode findKey(Integer key, boolean hol) throws IOException{
		
		//no lower bound, return very first leaf
		if (key == null && hol == true){
			return (LeafNode) deserializePage(1);
			
		}
		else if(key == null && hol == false)
		{
			return (LeafNode)deserializePage(numLeaves);
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
				
				int page = Collections.binarySearch(currIndex.getKeys(),key+1);
				page = (page >= 0) ? page : -(page+1);
				
				//Using index we found in the keylist, get the address of the next page to traverse to
				pageAddr = currIndex.getChildPages().get(page);
				
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
		
		LeafNode leaf = new LeafNode();
		leaf.setDataEntry(dataEntries);
		leaf.setPage(pageNum);
 
		Object[] arr = dataEntries.keySet().toArray();
		for(int i =0; i<arr.length; i++)
		{
			leaf.sort((int)arr[i]);//return leaf;
		}
	return null;
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
		
	}
	
	/**
	 * Sets global currLeaf to the next leaf node, and
	 * updates the keys enum and currkey to the first key in the enum.
	 * If we reach the last of the leaf nodes, it will set CurrLeaf to null
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
		
		bufferCtr++;
		if(bufferCtr-1 < ridlist.size())
		{
			return ridlist.get(bufferCtr-1);
		}
		else
		{
			return null;
		}
		//On the first call, initialize keys.
		//Find the first key that is greater than or equal to lowkey
	
		
	
		
	}
	
	

}