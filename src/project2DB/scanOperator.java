package project2DB;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import net.sf.jsqlparser.schema.Table;

/**
 * ScanOperator is the class representing the scan operator.
 * 
 * @author Robert Cao rrc8 Pulkit Kashyap pk374 Jason Zhou jz629
 */
public class scanOperator extends Operator{
	public String table = "";
	private DatabaseCatalog dbcat = DatabaseCatalog.getInstance();
	String fileUrl = "";
	Scanner scan;
	Tuple returnedTuple;
	tableProperties tableR;
	int tableCtr;
	int finalValue;
	File f;
	String alias;
	TupleReader reader;
	ArrayList<Integer> tuples;
	
	
	public scanOperator(Table tableN) 
	{
		alias = tableN.getAlias();
		table = tableN.getWholeTableName();
		if(table.substring(0,1).equals("."))
		{
			table = table.substring(1);
		}
		tableR = (tableProperties) dbcat.getTableCatalog().get(table);
		fileUrl = tableR.getUrl();
		
		try {
		reader = new TupleReader(fileUrl);
		}
		catch (IOException e)
		{
			System.out.println("Could not create a new scanOperator b/c file of reader threw exception.");
			e.printStackTrace();
		}
		
		tuples = new ArrayList<Integer>();
		f = new File(fileUrl);

	}
	
	/**
	 * getNextTuple() returns the next tuple of this scan operator, or an ENDOFFILE object if there are no more tuples
	 * @returns returnedTuple
	 */
	//@Override
	public Tuple getNextTuple() {
		//System.out.println(table);
		Tuple returnedTuple = new Tuple(table, alias, reader);
 		if (alias == null){
 
			returnedTuple.setName(table);
		}
         if(reader==null)
         {
        	 System.out.println("hehexd?23");
         }
		if (reader.getArrayList().isEmpty()) 
		{
 
			returnedTuple.setName("ENDOFFILE");
			System.out.println("endoffile");
		}

		return returnedTuple;
			
	}
	
	
	@Override
	/**
	 * Resets this operator such that getNextTuple() will return the first tuple it originally would.
	 */
	public void reset() {
		try {
			reader = new TupleReader(fileUrl);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	//@Override
	/**
	 * Dump prints out every tuple from the scan
	 */
	public void dump() {
		returnedTuple = getNextTuple();
		while(returnedTuple !=null)
		{
			for(int i =0; i <returnedTuple.getValues().size(); i++)
			{
				System.out.print(returnedTuple.getValues().get(tableR.getColumns().get(i)) + ",");
			}
			System.out.println();

			returnedTuple = getNextTuple();
		}

	}
	
	@Override
	public void reset(int index) {
		// TODO Auto-generated method stub
		
	}


}
