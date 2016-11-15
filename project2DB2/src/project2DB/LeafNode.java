package project2DB;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;

public class LeafNode extends TreeNode{	
   Hashtable <Integer, ArrayList<RId>> dataEntry = new Hashtable <Integer, ArrayList<RId>>();
   int smallestKey = Integer.MAX_VALUE;
   public int pageNumber;
   
   //Added Constructor 
   public LeafNode(Hashtable<Integer,ArrayList<RId>> entry, int pageNum){
	   this.dataEntry = entry;
	   this.pageNumber = pageNum;
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
		for(int i =0; i<dataEntry.keySet().size(); i++)
		{
			
			Object key = dataEntry.keySet().toArray()[i];
			ArrayList<RId> values = dataEntry.get(key);
			System.out.print(key+ ":[ ");
			for(int j = 0; j< values.size(); j++)
			{
				System.out.print("(" + values.get(j).pageId + ", " + values.get(j).tupleId + "); ");
			}
			System.out.print("]");
			System.out.println();
		}
		
	}
}
