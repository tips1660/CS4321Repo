package project2DB;

import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class TreeSerializer {
    BPlusTree tree;
    int order;
    ArrayList<ArrayList<IndexNode>> indexNodeLayering;
    ArrayList<IndexNode> firstIndexLevel;
    ArrayList<LeafNode> leafs;
    int rootAddress=0;
    private String fname = "";
	FileOutputStream fout;
	FileChannel fc;
	private ByteBuffer buffer = ByteBuffer.allocate(4096);
	private int numAttributes;
	private int numTuples;
	private int currentByte;
	private int startOfTuple;
	private Tuple tuple;
	private int currentPage = 0;
	private int pagesWritten =0;
	
	public TreeSerializer(BPlusTree indexTree, int d) {
		tree = indexTree;
		order = d;
		indexNodeLayering = tree.getIndexNodeList();
		firstIndexLevel  = tree.getIndexImmediateLayer();
		leafs = tree.getLeafNodeList();
		rootAddress+=leafs.size() + firstIndexLevel.size();
		for(int i =0; i< indexNodeLayering.size(); i++)
		{
			rootAddress+= indexNodeLayering.get(i).size();
		}
		currentPage = 0;
		currentByte = 0;
		while(currentPage < rootAddress){
		write();
		}
	}
	public void write()
	{
		if(currentPage == 0)
		{
			if(currentByte ==0)
			{
				buffer.putInt(currentByte, rootAddress);
				currentByte = 4;
				buffer.putInt(currentByte, leafs.size());
				currentByte = 8;
				buffer.putInt(currentByte, order);
				write();
				currentPage++;
			}
			
		}
	}
	

}
