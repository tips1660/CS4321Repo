package project2DB;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;

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
	String out;

	public TreeSerializer(BPlusTree indexTree, int d, String out) throws Exception {
		fout = new FileOutputStream(out);
		fc = fout.getChannel();
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
	public void write() throws IOException
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
				currentByte+=4;
				finishWriting();
			}

		}
		else
		{
			for(int i =0; i<leafs.size(); i++)
			{
				if(currentByte == 0)
				{
					buffer.putInt(currentByte, 0);
					currentByte+=4;
					buffer.putInt(currentByte, leafs.get(i).getDataEntry().keySet().size());
					currentByte+=4;
					
					Object[] keyArray = leafs.get(i).getDataEntry().keySet().toArray();
					ArrayList<Integer> keys = new ArrayList<Integer>();
					for(int j =0; j< keyArray.length; j++)
					{
					   keys.add((int)keyArray[j]);
					}
					Collections.sort(keys);
					for(int j=0; j<keys.size(); j++)
					{
						buffer.putInt(currentByte, keys.get(j));
						currentByte+=4;
						ArrayList<RId> pT = leafs.get(i).getDataEntry().get(keys.get(j));

						buffer.putInt(currentByte, pT.size()); 
						currentByte+=4;
						// puts the size of rids for that specific key
						for(int p = 0; p< pT.size(); p++)
						{
							buffer.putInt(currentByte, pT.get(p).getPageId());
							currentByte+=4;
							buffer.putInt(currentByte, pT.get(p).getTupleId());
							currentByte+=4;

						}
						leafs.get(i).pageNumber = currentPage;

					}
                   finishWriting();
				}
			}
			for(int i =0; i<firstIndexLevel.size(); i++)
			{
				
				if(currentByte == 0){
					firstIndexLevel.get(i).pageNumber= currentPage;

					buffer.putInt(currentByte, 1);
					currentByte+=4;
					buffer.putInt(currentByte, firstIndexLevel.get(i).getKeys().size());
					currentByte+=4;
					ArrayList<Integer> keys = firstIndexLevel.get(i).getKeys();
					ArrayList<TreeNode> children = firstIndexLevel.get(i).getChildren();
					Collections.sort(keys);
					for(int j =0; j< keys.size(); j++)
					{
						buffer.putInt(currentByte, keys.get(j));
						currentByte+=4;
					}
					for(int j=0; j<children.size(); j++)
					{
						buffer.putInt(currentByte, ((LeafNode)children.get(j)).pageNumber);
						currentByte+=4;
					}
					finishWriting();
				}
			}
 			for(int i = 0; i<indexNodeLayering.size(); i++)
			{
				for(int j =0; j<indexNodeLayering.get(i).size(); j++)
				{
 					if(currentByte == 0)
					{
						indexNodeLayering.get(i).get(j).pageNumber = currentPage;
						buffer.putInt(currentByte,1);
						currentByte+=4;
						buffer.putInt(currentByte, indexNodeLayering.get(i).get(j).getKeys().size());
						currentByte+=4;
						ArrayList<Integer> keys = indexNodeLayering.get(i).get(j).getKeys();
						ArrayList<TreeNode> children = indexNodeLayering.get(i).get(j).getChildren();
						Collections.sort(keys);
						System.out.println("there should be 15 keys: " + keys.size());
						for(int p=0; p< keys.size(); p++)
						{
							buffer.putInt(currentByte, keys.get(p));
							currentByte+=4;
						}
						System.out.println("there should be 16 children: " + children.size());
						for(int p =0; p < children.size(); p++){
							buffer.putInt(currentByte, ((IndexNode)children.get(p)).pageNumber);
							currentByte+=4;
						}
						finishWriting();
					}
				}
			}
		}

	}

	public void finishWriting() throws IOException
	{
		while(currentByte < 4096)
		{
			buffer.putInt(currentByte, 0);
			currentByte+=4;
		}
		fc.write(buffer);
		buffer.clear();
		currentByte=0;
		currentPage++;

	}

}

