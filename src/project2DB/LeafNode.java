package project2DB;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;

public class LeafNode extends TreeNode{	
	Hashtable <Integer, ArrayList<RId>> dataEntry = new Hashtable <Integer, ArrayList<RId>>();
	int smallestKey = Integer.MAX_VALUE;
	public int pageNumber;

	public LeafNode()
	{
		
	}
	
	public LeafNode(Hashtable <Integer, ArrayList<RId>> h, int p)
	{
		dataEntry = h;
		pageNumber = p;
	}
	
	public void sort(int key)
	{
		Collections.sort(dataEntry.get(key), RId.ridComparator);
	}
	public Hashtable<Integer, ArrayList<RId>> getDataEntry()
	{
		return dataEntry;
	}

	public int getSmallestKey()
	{
		return smallestKey;
	}
	public void setSmallestKey(int key)
	{
		this.smallestKey   = key;
	}

	public void printStuff()
	{
		ArrayList<Integer> keys = new ArrayList<Integer>();
		Object[] arr = dataEntry.keySet().toArray();
		for(int i =0; i<arr.length; i++)
		{
			keys.add((int)arr[i]);
		}
		Collections.sort(keys);
		for(int i =0; i<keys.size(); i++)
		{

			
			ArrayList<RId> values = dataEntry.get(keys.get(i));
			System.out.print(keys.get(i)+ ":[ ");
			for(int j = 0; j< values.size(); j++)
			{
				System.out.print("(" + values.get(j).pageId + ", " + values.get(j).tupleId + "); ");
			}
			System.out.print("]");
			System.out.println();
		}

	}
	public void setDataEntry(Hashtable<Integer, ArrayList<RId>> dataEntries) {
	    dataEntry = dataEntries;
		
	}
	public void setPage(int page)
	{
		pageNumber = page;
	}
	
	public int getAddress() {
		// TODO Auto-generated method stub
		return pageNumber;
	}
}


