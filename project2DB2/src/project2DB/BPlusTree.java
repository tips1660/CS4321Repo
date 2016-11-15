package project2DB;

import java.util.ArrayList;
import java.util.Set;

import com.sun.corba.se.impl.orbutil.graph.Node;

public class BPlusTree {
	ArrayList<Tuple> buffer;
	String key;
	int order;
	int ctr;
	int kCtr =0;
	int k;
	int uniqueKeysLeft = 0;
	boolean breakCondition = false;
	boolean updateNew;
	int indexCtr;
	boolean continueCreateIndexNodes = true;
	boolean stopIndexingLayeringReal = false;
	int numChildrenToBeAdded = 0;
	int arrayPtr  = 0; // what array are you currently checking out
	int lastIndex = 0; // where in the last arrayList did you end up
	int mCtr = 0; // for indexNodes special condition
	IndexNode root;

	ArrayList<LeafNode> leafLayer = new ArrayList<LeafNode>();
	ArrayList<IndexNode> indexLayer = new ArrayList<IndexNode>();
	ArrayList<ArrayList<IndexNode>> indexLayersReal = new ArrayList<ArrayList<IndexNode>>();
	public BPlusTree(ArrayList<Tuple> tupleList, String key, int d, int keysLeft)
	{
		buffer = tupleList;
		this.key = key;
		order = d;
		uniqueKeysLeft = keysLeft;

		while(ctr < buffer.size())
		{
			initializeLeafNodes();
			sortLeafs();
		}
		while(continueCreateIndexNodes)
		{
			initializeIndexNodes();
		}
		while(stopIndexingLayeringReal == false)
		{
			initialIndexNodesReal();
		}
		root = indexLayersReal.get(indexLayersReal.size()-1).get(0);
		printLeafsSecond();	 
		//printIndices(); 	
		//printRealIndices();
	}
	// call this repeatedly till ctr = buffer size
	public void initialIndexNodesReal()
	{
		IndexNode node = new IndexNode();

		if(arrayPtr == 0)
		{  

			node.getChildren().add(indexLayer.get(lastIndex));
			lastIndex++;

			while(lastIndex < indexLayer.size())
			{
				if(node.getKeys().size() < (2 * order))
				{
					node.getKeys().add(indexLayer.get(lastIndex).getSmallestKey());
					node.getChildren().add(indexLayer.get(lastIndex));
					lastIndex++;
				}
				else
				{
					break;
				}
			}
			if(indexLayersReal.size() > 0)
			{
				indexLayersReal.get(0).add(node);
			}
			else{
				indexLayersReal.add(new ArrayList<IndexNode>());
				indexLayersReal.get(0).add(node);
			}

			if(lastIndex == indexLayer.size()){
				arrayPtr++;
				lastIndex =0;
			}
		}
		else{
			if(indexLayersReal.get(arrayPtr-1).size() == 1){
				System.out.println("I got to this setting");
				stopIndexingLayeringReal = true;
			}
			else{


				while(lastIndex < indexLayersReal.get(arrayPtr-1).size())
				{

					node.getChildren().add(indexLayersReal.get(arrayPtr-1).get(lastIndex));
					lastIndex++;

					if(node.getKeys().size() < (2 * order))
					{
						node.getKeys().add(indexLayersReal.get(arrayPtr-1).get(lastIndex).getSmallestKey());
						node.getChildren().add(indexLayersReal.get(arrayPtr-1).get(lastIndex));
						lastIndex++;
					}
					else
					{
						break;
					}
				}
				if(indexLayersReal.size() > arrayPtr)
				{
					indexLayersReal.get(arrayPtr).add(node);
				}
				else
				{
					indexLayersReal.add(new ArrayList<IndexNode>());
					indexLayersReal.get(arrayPtr).add(node);
				}
				if(lastIndex == indexLayersReal.get(arrayPtr-1).size()){
					arrayPtr++;
					lastIndex =0;
				}



			}
		}

	}
	public void initializeIndexNodes()
	{	IndexNode node = new IndexNode();

	node.getChildren().add(leafLayer.get(indexCtr));
	indexCtr++;


	numChildrenToBeAdded = leafLayer.size()-indexCtr;
	if((numChildrenToBeAdded < (3 * order + 2)) && (numChildrenToBeAdded > (2 * order +1)))
	{
 		while(mCtr < numChildrenToBeAdded/2)
		{
			node.getKeys().add(leafLayer.get(indexCtr).getSmallestKey());
			node.getChildren().add(leafLayer.get(indexCtr));
			mCtr++;
			indexCtr++;
		}
		indexLayer.add(node);
		node = new IndexNode();
		mCtr=0;
		while(indexCtr < leafLayer.size())
		{
			node.getKeys().add(leafLayer.get(indexCtr).getSmallestKey());
			node.getChildren().add(leafLayer.get(indexCtr));
			indexCtr++;
		}
		indexLayer.add(node);
		continueCreateIndexNodes = false;
		indexCtr=0;
	}
	else
	{
		while(indexCtr< leafLayer.size()){
			if(node.getKeys().size() < (2 * order) )
			{   
				node.getKeys().add(leafLayer.get(indexCtr).getSmallestKey());
				node.getChildren().add(leafLayer.get(indexCtr));
				indexCtr++;
			}
			else
			{

				break;
			}
		}
		if(node.getKeys().size() > 0 )
		indexLayer.add(node);
		if(indexCtr == leafLayer.size())
		{
			continueCreateIndexNodes = false;
		}
	}

	// if done iterating as in you hit the end, you should 

	}

	public void initializeLeafNodes()
	{
		LeafNode node = new LeafNode();
		boolean kConditionCheck = kCondition();
		//to do: add functinoality to make this node take only k/2 total if it fulfills the final 2 condition.
		if(!kConditionCheck){
			while(!breakCondition && ctr < buffer.size())
			{
				node = updateNode(node,kConditionCheck);

			}
			
			leafLayer.add(node);
			breakCondition = false;
		}

		else
		{
			k = uniqueKeysLeft;

			// k condition holds so special logic for making my stuff           
			//after leaf layer i need to make index layer, traversal 

			// while i am at less than k/2 put that stuff in my 2nd to last node. then reset and put the rest in my final thing
			while(!breakCondition)
			{
				node = updateNode(node, kConditionCheck);
				if(kCtr == k/2){
					breakCondition = true;
					leafLayer.add(node);
					node = new LeafNode();
					kCtr = 0;
				}

			}

			breakCondition = false;

			// make a final leaf node that has the rest of the stuff
			while(ctr < buffer.size()){
				node = updateNode(node, false);
			}
			// add it
			leafLayer.add(node);
		}
	}

	public LeafNode updateNode(LeafNode node, Boolean kConditionCheck)
	{
		System.out.println(buffer.get(ctr).getValues() + " this is the: " + ctr + " tuple" );
		int keyValue = (int) buffer.get(ctr).getValues().get(key);
		int pageNum = buffer.get(ctr).getPage();
		int tupleNum = buffer.get(ctr).getTupleNum();
		if(node.getDataEntry().get(keyValue)!=null)
		{
			node.getDataEntry().get(keyValue).add(new RId(pageNum, tupleNum));
			if(keyValue < node.getSmallestKey())
				node.setSmallestKey(keyValue);
			ctr++;
		}
		else
		{
			if(!kConditionCheck)
			{
				if(node.getDataEntry().keySet().size()+1 > (2 * order))

				{
					breakCondition = true;
					System.out.println("INDEXCODE1:break condition is true, 20 keys hit");
				}
				else
				{
					node.getDataEntry().put(keyValue, new ArrayList<RId>());
					node.getDataEntry().get(keyValue).add(new RId(pageNum, tupleNum));
					uniqueKeysLeft--;
					ctr++;
					if(keyValue < node.getSmallestKey())
						node.setSmallestKey(keyValue);
				}
			}
			else
			{
				if(kCtr == k/2)
				{
					this.breakCondition = true;
					System.out.println("IndexCode2: Splitting the remaining goods");
				}
				else
				{
					node.getDataEntry().put(keyValue, new ArrayList<RId>());
					node.getDataEntry().get(keyValue).add(new RId(pageNum, tupleNum));
					uniqueKeysLeft--;
					kCtr++;
					ctr++;
					if(keyValue < node.getSmallestKey())
						node.setSmallestKey(keyValue);
				}
			}
		}

		return node;
	}

	 public void sortLeafs()
	 {
		 for(int i =0; i< leafLayer.size(); i++)
		 {
			 Object[] keys = leafLayer.get(i).getDataEntry().keySet().toArray();
			 for(int j =0; j<keys.length; j++)
			 {
				 leafLayer.get(i).sort((int)keys[j]);
			 }
		 }
	 }
	public boolean kCondition()
	{
		if(uniqueKeysLeft < (3 * order) && uniqueKeysLeft > (2 * order))
		{
			return true;
		}
		return false;
	}

	public void printLeafs()
	{
		int accumulator = 0;
		for(int i =0; i < leafLayer.size(); i++)
		{
			Set thisLeafSet = leafLayer.get(i).getDataEntry().keySet();
			int keysTotalforNodeI = thisLeafSet.size();
			System.out.println("leaf number: " + (i+1) + " key total: " + keysTotalforNodeI);
			for(int j =0; j <thisLeafSet.size(); j++)
			{
				int key  = (int) thisLeafSet.toArray()[j];
				int dataEntrySize = leafLayer.get(i).getDataEntry().get(key).size();
				accumulator+=dataEntrySize;
			}
		}
		System.out.println("total tuples captured: " + accumulator);
	}

	public void printIndices()
	{
		int accumulator = 0;
		for(int i =0; i< indexLayer.size(); i++)
		{
			int keyTotal = indexLayer.get(i).getKeys().size();
			System.out.println("for index node: " + (i+1) + " I have this many keys: " + keyTotal + " with 2*order being: " + (2*order));
			accumulator += indexLayer.get(i).getChildren().size();

		}
		
        for(int i =0; i< indexLayer.size(); i++)
        {
        	System.out.print("Here are all the keys for this index node: ");
        	for(int j =0; j<indexLayer.get(i).getKeys().size(); j++){
        		System.out.print(indexLayer.get(i).getKeys().get(j) + ", ");
        	}
        	System.out.println();
        	
        }
		System.out.println();
		LeafNode t=  new LeafNode();
		LeafNode p = ((LeafNode)(indexLayer.get(0).getChildren().get(0)));
	
		System.out.println("In total, I have these many leaves accounted for: " + accumulator);
		System.out.println("my k was: " + k);
		System.out.println("but my buffer size was: " + buffer.size());
	}

	public void printRealIndices()
	{
		System.out.println("the total number of levels I have are: " + (indexLayersReal.size() + 2));

		for(int i =0; i<indexLayersReal.size(); i++ )
		{
			int accumulator = 0;
			int keys = 0;
			for(int j =0; j< indexLayersReal.get(i).size(); j++)
			{
				accumulator +=indexLayersReal.get(i).get(j).getChildren().size();
				keys+=indexLayersReal.get(i).get(j).getKeys().size();
				System.out.print(indexLayersReal.get(i).get(j).getKeys() + " ");

			}
			System.out.println();
			System.out.println("the total number of things in this level that I have are: " + indexLayersReal.get(i).size());
			System.out.println("the total number of index nodes I have as kids on level: " + i + " are: " + accumulator);
			System.out.println("the total number of keys I have across my different index nodes on level: " + i + "are: " + keys);
		}
	}
	public void printLeafsSecond()
	{
		for(int i =0; i< leafLayer.size(); i++)
		{
			leafLayer.get(i).printStuff();
			System.out.println();
		}
	}
	public IndexNode getRoot()
	{
		return root;
	}
   public ArrayList<LeafNode> getLeafNodeList()
   {
	   return leafLayer;
   }
   public ArrayList<IndexNode> getIndexImmediateLayer()
   {
	   return indexLayer;
   }
   public ArrayList<ArrayList<IndexNode>> getIndexNodeList()
   {
	   return indexLayersReal;
   }
   
}
