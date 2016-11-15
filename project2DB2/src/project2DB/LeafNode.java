package project2DB;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;

public class LeafNode extends TreeNode{	
   Hashtable <Integer, ArrayList<RId>> dataEntry = new Hashtable <Integer, ArrayList<RId>>();
   int smallestKey = Integer.MAX_VALUE;
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
	
}