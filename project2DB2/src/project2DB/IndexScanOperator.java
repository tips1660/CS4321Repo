package project2DB;

import java.io.File;
import java.io.IOException;

import net.sf.jsqlparser.schema.Table;

public class IndexScanOperator extends Operator {
	
	public String table = "";
	String alias;
	int cluster;
	Integer lowkey = null;
	Integer highkey = null;
	File indexFile = null;
	
	
	public IndexScanOperator(Table relation, File indexFile,int cluster,Integer low, Integer high){
		alias = relation.getAlias();
		table = relation.getWholeTableName();
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
		
		
	
	}
	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
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
