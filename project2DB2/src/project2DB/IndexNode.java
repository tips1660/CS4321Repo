package project2DB;

import java.util.ArrayList;

public class IndexNode extends TreeNode{
	ArrayList<LeafNode> children = new ArrayList<LeafNode>();
	ArrayList<Integer> keys = new ArrayList<Integer>();

	public ArrayList<LeafNode> getChildren() {
		return children;
	}
	public void setChildren(ArrayList<LeafNode> children) {
		this.children = children;
	}
	public ArrayList<Integer> getKeys() {
		return keys;
	}
	public void setKeys(ArrayList<Integer> keys) {
		this.keys = keys;
	}
	
	

}
