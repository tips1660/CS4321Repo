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

	ArrayList<LeafNode> leafLayer = new ArrayList<LeafNode>();
	public BPlusTree(ArrayList<Tuple> tupleList, String key, int d, int keysLeft)
	{
		buffer = tupleList;
		this.key = key;
		order = d;
		uniqueKeysLeft = keysLeft;

		while(ctr < buffer.size())
		{
			initializeLeafNodes();
		}
		printLeafs();
	}
	// call this repeatedly till ctr = buffer size
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

			}
			leafLayer.add(node);
			breakCondition = false;
			node = new LeafNode();
			kCtr=0;
			// make a final leaf node that has the rest of the stuff
			while(ctr < buffer.size()){
				node = updateNode(node, kConditionCheck);
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
				}
			}
			else
			{
				if(kCtr + 1 > k/2)
				{
					breakCondition = true;
					System.out.println("IndexCode2: Splitting the remaining goods");
				}
				else
				{
					node.getDataEntry().put(keyValue, new ArrayList<RId>());
					node.getDataEntry().get(keyValue).add(new RId(pageNum, tupleNum));
					uniqueKeysLeft--;
					kCtr++;
					ctr++;
				}
			}
		}

 		return node;
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

}
