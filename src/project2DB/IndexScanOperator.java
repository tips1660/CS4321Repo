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
		System.out.println("Entering IndexScanOperator constructor");
		alias = relation.getAlias();
		table = relation.getWholeTableName();
		tableR = (tableProperties) dbcat.getTableCatalog().get(table);
		fileUrl = tableR.getUrl();
		tuples = new ArrayList<Integer>();

		
		this.indexFile = indexFile;
		this.cluster = cluster;
		lowkey = low;
		highkey = high;
		System.out.println("Entering treedeserializer cons");
		td = new TreeDeserializer(indexFile,lowkey,highkey);
		System.out.println("finished treeserahakltaw cons");

		
		//Set the reader based on existence of lowkey
		if (lowkey == -1){
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
			/*try {
				System.out.println("about to get the next RId");
				RId first = td.getNextRId();
				System.out.println("got the next RId");
				if (first == null)
					System.out.println("first is null");
				System.out.println("page id is "+first.getPageId());
				reader = new TupleReader(fileUrl,first.getPageId(),first.getTupleId());
				System.out.println("reader was created");
				}
				catch (IOException e)
				{
					System.out.println("Could not create a new indexscanOperator");
					e.printStackTrace();
				}	*/
		}
		
		
		f = new File(fileUrl);
		System.out.println("finished the indexscan operator constructor");
	}
	
	
	
	
	@Override
	public Tuple getNextTuple(){
		
		System.out.println("xd");
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
			
				TupleReader readerGetNext;
				System.out.println("rid of the tuple reader thingo is "+RIdNext.getPageId());
				try
				{
					System.out.println(fileUrl);
					readerGetNext = new TupleReader(fileUrl, RIdNext.getPageId(), RIdNext.getTupleId());
					Tuple returnedTuple = new Tuple(table, alias, readerGetNext);
					readerGetNext.close();
					System.out.println("values of returnedtuple are "+returnedTuple.getValues());
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
		td.bufferCtr = 0;
		
	}

	@Override
	public void reset(int index) {
		// TODO Auto-generated method stub
		td.bufferCtr = index;
		
	}

	@Override
	public void dump() throws IOException {
		// TODO Auto-generated method stub
		
	}

}