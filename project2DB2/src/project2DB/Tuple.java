package project2DB;
import java.io.IOException;
import java.util.*;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * Tuple represents an entry within a table. 
 * 
 * @author Robert Cao rrc8 Pulkit Kashyap pk374
 */
public class Tuple{

	private Hashtable<String, Integer> values = new Hashtable<String, Integer>();
	private DatabaseCatalog cat = DatabaseCatalog.getInstance();
	String table2;
	private List<OrderByElement> orderList;
	private TupleReader reader;
	private ArrayList<String> itemList = new ArrayList<String>();
	private int page;
	private int tupleNum;
	
	/*
	 * Deprecated
	 */
	public Tuple(String tableName, String line, String alias)
	{
		try{

			tableProperties table = (tableProperties) cat.getTableCatalog().get(tableName);
			reader = new TupleReader(table.getUrl());
			table2  = tableName;
			parse(table, alias);
			//parse(line, table, alias);
		}
		catch(Exception e){
			System.out.println("Table does not exist1");
			System.out.println(e);
		}
	}
	
	public Tuple(String tableName, String alias, TupleReader r) {
		try{

			tableProperties table = (tableProperties) cat.getTableCatalog().get(tableName);
			reader = r;
			
			table2  = tableName;
			parse(table, alias);
		}
		catch(Exception e){
			System.out.println("Table does not exist2");
			System.out.println(e);
		}
	}
	
	public Tuple(TupleReader r) {
		reader = r;
		setName("ESORT");
		try {
			parseSort();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Tuple() {
		values = null;
	}
	
	public void parse(tableProperties table, String alias) throws IOException
	{
		ArrayList<Integer> tuples = new ArrayList<Integer>();
		tuples = reader.getNextTuple();
		page = reader.getPage();
		tupleNum = reader.getTupleNum();
		for (int i = 0; i < tuples.size(); i++) {
			if (alias != null) {
				values.put(alias+"."+(String)table.getColumns().get(i), tuples.get(i));
			}
			else {
				values.put(table2+"."+(String)table.getColumns().get(i), tuples.get(i));
			}
		}
		
	}
	public int getPage()
	{
		return page;
	}
	public int getTupleNum()
	{
		return tupleNum;
	}
	public void parseSort() throws IOException
	{
		ArrayList<Integer> tuples = new ArrayList<Integer>();
		tuples = reader.getNextTuple();
		for (int i = 0; i < tuples.size(); i++) {
				values.put("ESORT."+i, tuples.get(i));
		}
		if (tuples.isEmpty())
			setName("ENDOFFILE");
	}
	
	
	public void changeColumns() throws IOException
	{
		int value;
		for (int i = 0; i < getValues().size(); i++) {
			value = values.remove("ESORT."+i);
			values.put(itemList.get(i), value);
		}
	}

	/**
	 * Parses the tuple, matching the correct columns together as well as the alias
	 * 
	 * @param line, table, alias
	 */
	public void parse(String line, tableProperties table, String alias)
	{  int ctr=0;
	int leftPtr =0;

	for(int i = 0; i < line.length(); i++)
	{
		if(line.charAt(i) == ',')
		{
			if(alias!=null)
			{
				values.put(alias+"."+(String)table.getColumns().get(ctr), Integer.parseInt(line.substring(leftPtr, i)));

			}
			else
			{
				values.put(table2+"."+(String)table.getColumns().get(ctr), Integer.parseInt(line.substring(leftPtr, i)));
			}
			leftPtr = i+1;
			ctr++;
		}
	}
	if(alias!=null)
	{
		values.put(alias+"."+(String)table.getColumns().get(ctr), Integer.parseInt(line.substring(leftPtr, line.length())));
		table2=alias;

	}
	else
	{
		values.put(table2+"."+(String)table.getColumns().get(ctr), Integer.parseInt(line.substring(leftPtr, line.length())));
	}


	}
	
	/**
	 * Sets the orderList
	 * 
	 * @param inputlist
	 * 
	 * a new orderlist
	 */
    public void setOrderList(List<OrderByElement> inputList)
    {
    	orderList = inputList;
    }
    public void setOutputOrder(ArrayList<String> inputList)
    {
    	itemList = inputList;
    }
    
    /**
     * Gets the values of the columns of the tuple
     * @return values
     * the values of the columns of the tuple
     */
	public Hashtable getValues()
	{
		return values;
	}

	/**
	 * Sets the values of the columns of the tuple
	 * 
	 * @param valReplacement
	 * a new column value list 
	 */
	public void setValues(Hashtable<String, Integer> valReplacement)
	{
		values = valReplacement;
	}
	
	/**
	 * Gets the name of the tuple's table
	 * 
	 * @return table2
	 * the name of the tuple's table
	 */
	public String getTable()
	{
		return table2;   
	}
	
	/**
	 * Sets the name of the tuple's table
	 * @param s
	 * the name of the tuple's table
	 */
	public void setName(String s)
	{
		table2 = s;
	}
	
	public ArrayList<String> getItemList()
	{
		return itemList;
	}
	/*
	 * in the operator itself -> you wlll parse the file and as you read each line, you call tuple on the table name
	 * tuple will return a new tuple with the approrpaite column stuff and yea
	 */

	
}
