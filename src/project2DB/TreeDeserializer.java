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
	private ArrayList<RId> ridlist = new ArrayList<RId>();
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
	
		System.out.println("entering the treedeserializer from inside cons");
		this.indexFile = indexFile;
		this.lowkey = lowkey;
		this.highkey = highkey;
		leaves = new ArrayList<LeafNode>();
		System.out.println("about to setup buffer");
		buffer = ByteBuffer.allocate(4096);
		System.out.println(indexFile.getAbsolutePath());
		fs = new FileInputStream(indexFile);
		System.out.println("set up input stream");
		fc = fs.getChannel();
		extractHeaderData();
		currLeaf = findKey(lowkey, true);
		finalLeaf = findKey(highkey, false);
		System.out.println("both find keys have been found");
		System.out.println("high key is "+highkey);
		System.out.println("lowkey is "+lowkey);
		if (currLeaf == null)
			System.out.println("lol");
		if (finalLeaf == null)
			System.out.println("finalLeaf is null");
		currLeafNodeAddr = currLeaf.pageNumber;
		currKey= 0;
		currRId = 0;
		System.out.println("b4 extract nodes");
		extractNodes();
		extractRIDs();
			
	}
	public void extractRIDs()
	{
		System.out.println("starting extractRIDS");
		System.out.println("leaves size is " + leaves.size());
		int tempHigh = highkey;
		if (highkey == -1)
			tempHigh = Integer.MAX_VALUE;
		for(int i =0; i< leaves.size(); i++)
		{
			Object[] arr = leaves.get(i).getDataEntry().keySet().toArray();
			ArrayList<Integer> arr1 = new ArrayList<Integer>();
              for(int j=0; j<arr.length; j++)
              {
            	  arr1.add((int)arr[j]);
              }
              
			Collections.sort(arr1);
			for(int j =0; j< arr1.size(); j++)
			{
				int keyVal = (int)arr1.get(j);
				if(keyVal>= lowkey && keyVal <= tempHigh)
				{
					System.out.println("the value of this keyval is " +keyVal+" h");
					ArrayList<RId> myRId = leaves.get(i).getDataEntry().get(keyVal);
					System.out.println("the size of myRId is "+myRId.size());
					for(int p =0; p < myRId.size(); p++)
					{
						System.out.println("added an rid");
						if (p == 0)
							System.out.println("the first rid is "+myRId.get(p).pageId);
						ridlist.add(myRId.get(p));
					}
				}
			}
		}
		System.out.println("exiting extractRIDS");
	}
	/*
	 * get all the nodes in between my low key node and my high key node.
	 */
	public void extractNodes() throws IOException
	{   
		System.out.println("starting extractNodes");
		leaves.add(currLeaf);
		int currentPage = currLeaf.getAddress();
		currentPage++;
		System.out.println("the page of currLeaf is "+currentPage);
		System.out.println("the page of finalLeaf is "+finalLeaf.getAddress());
		while(currentPage < finalLeaf.getAddress())
		{
			LeafNode l = (LeafNode)deserializePage(currentPage);
			leaves.add(l);
			currentPage++;
		}
		leaves.add(finalLeaf);
		System.out.println("size of leaves is " + leaves);
		System.out.println("ending extractNodes");
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
		System.out.println("Starting findkey");
		//no lower bound, return very first leaf
		if (key == -1 && hol == true){
			System.out.println("low key is null");
			return (LeafNode) deserializePage(1);
			
		}
		else if(key == -1 && hol == false)
		{
			System.out.println("high key is null");
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
			System.out.println("exiting findkey");
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
		int nodeType = buffer.getInt(0);
		System.out.println("The node type of this page is "+nodeType);
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
	return leaf;
	}
	
	
	//should only be called once in the constructor.
	//Extracts header information from the serialized index file.
	//Clear buffer afterwards
	private void extractHeaderData() throws IOException {
		System.out.println("entering extractheader");
		buffer.clear();
		fc.read(buffer);
		rootAddress = buffer.getInt(0);
		numLeaves = buffer.getInt(4);
		order = buffer.getInt(8); 
		buffer.clear();
		System.out.println("the page of the root is " + rootAddress);
		System.out.println("the number of leave is " + numLeaves);
		System.out.println("the order of this tree is " + order);
		System.out.println("finished extractheader");
	}
	
	/**
	 * 
	 * @param pageID
	 * @throws Loads page PageID of the index file into the buffer
	 */
	private void getPage(int pageID) throws IOException{
		buffer.clear();
//		buffer.put(new byte[4096]);
//		buffer.clear();
		System.out.println("PageID in getpage is: "+pageID);
		
		long index = 4096 * (pageID);
		fc.position(index);
		//System.out.println("index is " +index);
		//System.out.println(fc.position());
		//System.out.println(fc.size());
		fc.read(buffer);
		//System.out.println("Node type : "+buffer.getInt(0));
		//System.out.println("Num Chilren : "+buffer.getInt(4));

		
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
		
		System.out.println("the value of bufferCtr is "+bufferCtr);
		System.out.println("the size of ridlist is " +ridlist.size());
		bufferCtr++;
		if (ridlist == null)
			System.out.println("ridlist is null");
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