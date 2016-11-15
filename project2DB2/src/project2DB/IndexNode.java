package project2DB;

import java.util.ArrayList;

public class IndexNode extends TreeNode{
	ArrayList<TreeNode> children = new ArrayList<TreeNode>();
	ArrayList<Integer> keys = new ArrayList<Integer>();
    int smallest  = Integer.MAX_VALUE;
	public ArrayList<TreeNode> getChildren() {
		return children;
	}
	public void setChildren(ArrayList<TreeNode> children) {
		this.children = children;
	}
	public ArrayList<Integer> getKeys() {
		return keys;
	}
	public void setKeys(ArrayList<Integer> keys) {
		this.keys = keys;
	}
	@Override
	public int getSmallestKey() {
		 for(int i =0; i < keys.size(); i ++)
		 {
			 if(keys.get(i) < smallest)
				 smallest = keys.get(i);
			 
		 }
		 return smallest;
	}
	
	

}
