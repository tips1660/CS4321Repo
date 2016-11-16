package project2DB;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
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
	TreeDeserializer td;
	String attr;
	
	/**
	 * 
	 * @param relation the name of the relation you want to scan 
	 * @param indexFile the index file of the relation that you want to use
	 * @param attr	the name of the attribute the relation is indexed by
	 * @param cluster	1 for clustered index,  0 for unclustered index
	 * @param low	lowkey, null if not needed
	 * @param high	highkey, null if not needed
	 * @throws IOException
	 */
	public IndexScanOperator(Table relation, File indexFile, String attr, int cluster,Integer low, Integer high) throws IOException{
		
		alias = relation.getAlias();
		table = relation.getWholeTableName();
		tableR = (tableProperties) dbcat.getTableCatalog().get(table);
		fileUrl = tableR.getUrl();
		tuples = new ArrayList<Integer>();

		
		this.indexFile = indexFile;
		this.cluster = cluster;
		lowkey = low;
		highkey = high;
		td = new TreeDeserializer(indexFile,lowkey,highkey);

		
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
				RId first = td.getNextRId();
				reader = new TupleReader(fileUrl,first.getPageId(),first.getTupleId());
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
	public Tuple getNextTuple(){
		
		
		//unclustered implementation
		if (cluster == 0){
			RId RIdNext = null;
			try {
				RIdNext = td.getNextRId();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			if (RIdNext == null){
				Tuple returnedTuple = new Tuple(table, alias, reader);
				returnedTuple.setName("ENDOFFILE");
				return returnedTuple;
			}
			else{
				//TODO: Need a way to read the tuple directly from the DB file given the RID
				//
				//HELP!!! 
				TupleReader readerGetNext;
				try
				{
					readerGetNext = new TupleReader(fileUrl, RIdNext.getPageId(), RIdNext.getTupleId());
					Tuple returnedTuple = new Tuple(table, alias, readerGetNext);
					if(alias == null) returnedTuple.setName(table);
					
					return returnedTuple;
					
				}catch(IOException e)
				{
					e.printStackTrace();
				}
				
				Tuple returnedTuple = new Tuple(table, alias, reader);
				returnedTuple.setName("ENDOFFILE");
				return returnedTuple;
			}
			
			
		}
		else{
			//We simply scan through the normal db file until we reach highkey or EOF			
			Tuple returnedTuple = new Tuple(table, alias, reader);
			if (alias == null)returnedTuple.setName(table);
			
			//check if reached end OR reached a tuple greater than highkey. END
			if (reader.getArrayList().isEmpty() || (int)returnedTuple.getValues().get(attr) > highkey) {
				returnedTuple.setName("ENDOFFILE");
			}
			return returnedTuple;
			
		}
		
		
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