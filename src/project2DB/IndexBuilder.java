package project2DB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * Created by Pulkit Kashyap pk374
 * Scans a file, and creates an index based on the desired attribute.
 */
public class IndexBuilder {
	

	int clustered;
	ArrayList<Tuple> tupleBuffer = new ArrayList<Tuple>();
	SortOperator tableSorter;
	ProjectOperator tableProjecter;
	scanOperator tableScanner;
	ProjectOperator tableProjecter2;
	scanOperator tableScanner2;
	SortOperator tableSorter2;
	BPlusTree indexTree; 
	Hashtable<Integer, Integer> totalUnique = new Hashtable<Integer, Integer>();
	String key;
	String relation;
	tableProperties tableR;
	String fileUrl;
	DatabaseCatalog dbcat = DatabaseCatalog.getInstance();	

	/*
	 * Build a sort operator, scan the table you want, sort based on the attribute, and then get the buffer.
	 */
	public IndexBuilder(int clustered, String out, String table, String schema, String attribute, int d) throws Exception
	{
		// need to add another attribute to tell me the file location of the table if clutsering so i can rewrite
		tableR = (tableProperties) dbcat.getTableCatalog().get(table);
		fileUrl = tableR.getUrl();

		key = table + "." + attribute;
		this.clustered= clustered;
		tableSorter = new SortOperator(null, null);
		tableSorter2= new SortOperator(null, null);
		Table tableN = new Table("", table);
		System.out.println(tableN.getWholeTableName());
		
		if(clustered == 1){
			tableProjecter = new ProjectOperator(null, null, tableN);
			tableScanner = new scanOperator(tableN);
			tableProjecter.setChild(tableScanner);
			tableSorter.setChild(tableProjecter);
			tableSorter.addToSortListActual(key);
			tableSorter.setupBuffer();
			System.out.println("am i going to CLUSTERING?");
			tableSorter.setTupleWriter(new TupleWriter(fileUrl));
			tableSorter.dump();
		}
		tableProjecter2 = new ProjectOperator(null, null, tableN);
		tableScanner2 = new scanOperator(tableN);
		tableProjecter2.setChild(tableScanner2);
		tableSorter2.setChild(tableProjecter2);
		tableSorter2.addToSortListActual(key);
		tableSorter2.setupBuffer();
		
		tupleBuffer = tableSorter2.getBuffer();
		
		for(int i =0; i<tupleBuffer.size(); i++)
		{
			totalUnique.putIfAbsent((int)tupleBuffer.get(i).getValues().get(key), 1);
		}
		System.out.println("this is how many unique S.A there are: " + totalUnique.keySet().size());
		indexTree = new BPlusTree(tupleBuffer, key, d, totalUnique.keySet().size());
		
		TreeSerializer serialize = new TreeSerializer(indexTree, d, out);
		
	}
	public BPlusTree getTree()
	{
		return  indexTree;
	}
	
	
	
	


}
