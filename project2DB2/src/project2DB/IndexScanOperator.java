package project2DB;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import tree.*;
import net.sf.jsqlparser.schema.Table;

public class IndexScanOperator extends Operator {
	
	public String table = "";
	String alias;
	private DatabaseCatalog dbcat = DatabaseCatalog.getInstance();
	String fileUrl = "";
	Tuple returnedTuple;
	tableProperties tableR;
	int tableCtr;
	int finalValue;
	File f;
	ArrayList<Integer> tuples;
	

	int cluster;
	Integer lowkey = null;
	Integer highkey = null;
	File indexFile = null;
	TupleReader reader;
	
	public IndexScanOperator(Table relation, File indexFile,int cluster,Integer low, Integer high) throws IOException{
		
		alias = relation.getAlias();
		table = relation.getWholeTableName();
		tableR = (tableProperties) dbcat.getTableCatalog().get(table);
		fileUrl = tableR.getUrl();
		tuples = new ArrayList<Integer>();

		
		this.indexFile = indexFile;
		this.cluster = cluster;
		lowkey = low;
		highkey = high;
		
		/*
		either upon construction or upon first call to getNextTuple() – you will need to access the index file, 
		navigate root-to-leaf to find lowkey (or where lowkey would be if it were in the tree, 
		since it may not be present) and grab the next data entry from the leaf. 
		Note that this root-to-leaf descent will involve deserializing a number of nodes in the tree; 
		do not deserialize the whole tree, deserialize only the pages that you need
		*/
		
		TreeDeserializer td = new TreeDeserializer(indexFile,lowkey,highkey);

		
		//Set the reader based on existence of lowkey
		if (lowkey == null){
			try {
				reader = new TupleReader(fileUrl);
				}
				catch (IOException e)
				{
					System.out.println("Could not create a new indexscanOperator");
					e.printStackTrace();
				}
		}
		else{
			try {
				reader = new TupleReader(fileUrl,pid,tid);
				}
				catch (IOException e)
				{
					System.out.println("Could not create a new indexscanOperator");
					e.printStackTrace();
				}	
		}
		

		
		
		
		f = new File(fileUrl);
		
	
	}
	
	
	
	
	@Override
	public Tuple getNextTuple() {
		
		/*
		If the index is unclustered, each call must examine the current data entry, 
		find the next rid, resolve that rid to a page and a tuple within the data file, 
		retrieve the tuple from the data file, and return it. If the index is clustered, 
		you must scan the (sorted) data file itself sequentially rather than going through 
		the index for each tuple. Thus you don’t need to scan any index pages after the 
		initial root-to-leaf descent that gives you the rid of the first matching tuple.
		*/
		
		//unclustered implementation
		if (cluster == 0){
			
		}
		//clustered implementation
		else{
			//We simply scan through the normal db file until we reach highkey or EOF
			//Todo: handle highkey for clusters
			
			Tuple returnedTuple = new Tuple(table, alias, reader);
			if (alias == null)
				returnedTuple.setName(table);
			if (reader.getArrayList().isEmpty()) {
				returnedTuple.setName("ENDOFFILE");
				//System.out.println("endoffile");
			}
			//System.out.println(returnedTuple.getValues().keySet());
			return returnedTuple;
			
		}
		return null;
		
		
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reset(int index) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dump() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
