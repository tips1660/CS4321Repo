package project2DB;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * SortOperator is the class representing the sort operator.
 * 
 * @author Robert Cao rrc8 Pulkit Kashyap pk374 Jason Zhou
 */
public class SortOperator extends SortOperatorParent  {

	ArrayList<Tuple> buffer = new ArrayList<Tuple>();
	List<OrderByElement> sortList;
	static ArrayList<String> sortListActual;
	ArrayList<String> sortListActualNonStatic;
	int ctr=0;
	ArrayList<SelectItem> items;
	ArrayList<String>columnList;
	private TupleWriter writer;
	private Table table;
	boolean bufferInit = false;
	int ctrBuffer =0;
	AllColumns asterik = new AllColumns();


	public SortOperator(String out, ArrayList<SelectItem> items1, List<OrderByElement> orderList) throws IOException
	{    
		System.out.println("i got to this constructor");

		items = items1;
		writer = new TupleWriter(out);
		sortList = orderList;
		if(sortList==null)
			sortList = new ArrayList<OrderByElement>();
		sortListActualNonStatic = new ArrayList<String>();
		//ProjectOperator childOp = new ProjectOperator(items, joinList, select, tableN);
		//this.setChild(childOp);		
		if(this.items == null)
		{
			items = new ArrayList<SelectItem>();
			items.add(asterik);
		}


	}

	public SortOperator(ArrayList<SelectItem> items1, List<OrderByElement> orderList) throws IOException
	{    System.out.println("got to my sort operator constructor");
	items = items1;
	//writer = null;
	sortList = orderList;
	if(sortList==null)
		sortList = new ArrayList<OrderByElement>();
	sortListActualNonStatic = new ArrayList<String>();
	///ProjectOperator childOp = new ProjectOperator(items, joinList, select, tableN);
	//this.setChild(childOp);		
	if(this.items == null)
	{
		items = new ArrayList<SelectItem>();
		items.add(asterik);
	}

	}
	/**
	 * 
	 * @return this sortoperator's table
	 */
	public Table getTable()
	{
		return table;
	}
	/**
	 * set this sort operator's table. 
	 */
	public void setTable(Table t)
	{
		System.out.println("this things table is being set right");
		table = t;
	}

	public ArrayList<Tuple> getBuffer()
	{
		return buffer;
	}
	/**
	 * Adds a new OrderByElement to the list containing them
	 * 
	 * @param op 
	 * 			OrderByElement to be added
	 */
	public void addToSortList(OrderByElement op)
	{
		if(sortList==null)
			sortList = new ArrayList<OrderByElement>();
		sortList.add(op);
	}

	/**
	 * gets the list of columns in order
	 * 
	 * @returns columnList
	 * 		the list of columns
	 */
	public ArrayList<String> getColumnList()
	{
		return columnList;
	}


	public static Comparator<Tuple> TupleComparator = new Comparator<Tuple>()
	{
		@Override
		/**
		 * Compares two tuples
		 * 
		 * @param o1, o2 
		 * Tuples
		 * @return value
		 * -1 if o1 < o2, 1 if o1 > o2, 0 if o1 = o2
		 */
		public int compare(Tuple o1, Tuple o2) {
			for(int i =0; i<sortListActual.size(); i++){
				//System.out.println("sorting on col: " + sortListActual.get(i));
			}

			for(int i =0; i< sortListActual.size(); i++)
			{  
				try {
					Integer a = (Integer) o1.getValues().get(sortListActual.get(i));
					Integer b = (Integer) o2.getValues().get(sortListActual.get(i));

					if(a.intValue()<b.intValue())
						return -1;
					else if(a.intValue()>b.intValue())
						return 1;
					else
						continue;
				} catch(Exception e){
					System.out.println("COLUMN ERROR");
					//means this column isn't in my tuple
					continue;	
				}
			}
			return 0;
		}
	};

	/**
	 * getNextTuple() returns the next tuple of this sort operator, or an ENDOFFILE object if there are no more tuples
	 * 
	 * @returns returnedTuple
	 */
	@Override
	public Tuple getNextTuple() {
		if(bufferInit == false)
		{
			setupBuffer();

			bufferInit = true;
		}
		if(ctr < buffer.size())
		{  
			ctr++;
			return buffer.get(ctr-1);
		}
		else
		{
			Tuple temp = new Tuple();
			temp.setName("ENDOFFILE");
			return temp;
		}

	}

	/**
	 * 
	 * @param s -> adds this parameter to the sort list.
	 */
	public void addToSortListActual( String s)
	{
		sortListActualNonStatic.add(s);
	}
	/**
	 * sets up the buffer, then sorts it based on the conditions
	 */
	public void setupBuffer()
	{
		System.out.println("got to my buffer set up");
		bufferInit = true;

		columnList = ((ProjectOperator)this.getChild()).getColumnList();
		System.out.println("got to my buffer set up2");

		for(int i =0; i< sortList.size(); i++)
		{
			sortListActualNonStatic.add(sortList.get(i).toString());

		}
		if(columnList == null)
			System.out.println("ES NULLL");
		for(int i = 0; i<columnList.size(); i++)
		{
			if(sortListActualNonStatic.contains(columnList.get(i)))
				continue;
			else
				sortListActualNonStatic.add(columnList.get(i));

		}
		System.out.println("this is my current sort order: " + sortListActualNonStatic);

		Tuple currentTuple = child.getNextTuple();
		System.out.println("am i going into the right stuff");
 		while(true)
		{
			if(currentTuple == null){
				currentTuple= child.getNextTuple();
				while(currentTuple==null){
					currentTuple = child.getNextTuple();
					System.out.println("keep getting nulls for days");
				}
				if(currentTuple.getValues().keySet().size() > 5)
				{
					System.out.println("Gaga: " + currentTuple.getValues());
				}

			}
			else
			{  System.out.println("aint null");
			
				if(currentTuple.getTable()==null)
					System.out.println("no table");


				if(currentTuple.getTable().equals("ENDOFFILE")){
					System.out.println("not coming here right?");
					break;
				}
				else{
					System.out.println("buffer: " + currentTuple.getValues());
 					buffer.add(currentTuple);
					currentTuple = child.getNextTuple();
					while(currentTuple == null)
						currentTuple=child.getNextTuple();



				}

			}
			System.out.println("wile loop check");

		}

		try {

			sortListActual = new ArrayList<String>();
			for(int i =0; i< sortListActualNonStatic.size(); i++)
			{
				sortListActual.add(sortListActualNonStatic.get(i));
			}
			System.out.println(sortListActualNonStatic);
			System.out.println("presorting check32: " +sortListActual);
			System.out.println("the size of the buffer is "+buffer.size());
			Collections.sort(buffer, TupleComparator);
		}
		catch(Exception e) {
			System.out.println("caught an exception sorting this");
		}
	}
	public void setTupleWriter(TupleWriter t)
	{
		writer = t;
	}

	//@Override
	/**
	 * Dump prints out every tuple from the sort
	 */
	public void dump() {


		if(bufferInit == false)
			setupBuffer();

		for(int i =0; i<buffer.size(); i++)
		{
			Tuple newTuple = new Tuple();
			ArrayList<String> itemList = new ArrayList<String>();
			Hashtable<String, Integer> newValues = new Hashtable<String, Integer>();
			//System.out.println("h");
			System.out.println(buffer.get(i).getValues().values());
			//System.out.println("getting tuples from writer");
			if(!(items.get(0) instanceof AllColumns)){
				for(int j =0; j < items.size()-1; j++)
				{   
					String colName = ((Column)((SelectExpressionItem) items.get(j)).getExpression()).toString();
					itemList.add(colName);
					newValues.put(colName, (Integer) buffer.get(i).getValues().get(colName));
					System.out.print(buffer.get(i).getValues().get(colName) + ",");
				}
				String colName = ((Column)((SelectExpressionItem) items.get(items.size()-1)).getExpression()).toString();
				itemList.add(colName);
				newValues.put(colName, (Integer) buffer.get(i).getValues().get(colName));
				System.out.println(buffer.get(i).getValues().get(colName));


				//useful stuff
				buffer.get(i).setOutputOrder(itemList);
			}
			else{
				for(int j =0; j <columnList.size()-1; j++) {
					String colName = columnList.get(j);
					newValues.put(colName, (Integer) buffer.get(i).getValues().get(colName));
					System.out.print(buffer.get(i).getValues().get(colName) + ",");

				}
				String colName = columnList.get(columnList.size()-1);
				newValues.put(colName, (Integer) buffer.get(i).getValues().get(colName));
				System.out.println(buffer.get(i).getValues().get(colName));
				buffer.get(i).setOutputOrder(columnList);
			}
			newTuple.setValues(newValues);

			//System.out.println(newValues.values());
			try {
				writer.writeTuple(buffer.get(i));
			} 
			catch (IOException e) {
				System.out.println("could not get tuples");
				e.printStackTrace();
			}
		}
		try {
			writer.write();
			System.out.println(writer.getNumAttributes());
		} catch (IOException e) {
			System.out.println("could not write");
			e.printStackTrace();
		}
		//at this point my buffer should have everything and we now need to sort it.
	}
	public void sort() throws IOException {
		// TODO Auto-generated method stub

	}


	@Override
	public void reset() {

	}

	public void reset(int index){
		ctr=index;
	}




}
