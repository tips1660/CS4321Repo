package project2DB;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
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

public class DuplicateEliminationOperator extends Operator {

	static List<OrderByElement> sortList;
	int ctr=0;
	ArrayList<SelectItem> items;
	ArrayList<String>columnList;
	Tuple prevTuple;
	Tuple currentTuple;
	HashSet<String> hashedTuples= new HashSet<String>();
	ArrayList<String> tupleList = new ArrayList<String>();
	String tupleValueString;
	private TupleWriter writer;
	int sortType;
	int sortBuffer;

	public DuplicateEliminationOperator(String out, ArrayList<SelectItem> items1, int s1, int s2) throws IOException
	{
		items= items1;
		writer = new TupleWriter(out);
		sortType = s1;
		sortBuffer = s2;
		
		prevTuple = null;
	}

	public String tupleToString(Tuple stringTuple)
	{
		if(stringTuple == null)
			return "";
		String finalResult = "";
		for(int i =0; i<stringTuple.getValues().size();i++)
		{
			String key = stringTuple.getValues().keySet().toArray()[i].toString();
			finalResult+=stringTuple.getValues().get(key).toString();
		}
		return finalResult;
	}
	@Override
	public Tuple getNextTuple() {
		System.out.println("got here again");
		//if my child is sort I have tuples coming in, in sorted order 
		
		if (sortType == 0 && child instanceof SortOperator) {
			if (prevTuple == null) {
				prevTuple = child.getNextTuple();
				currentTuple = prevTuple;
				return prevTuple;
			}
			else {

				prevTuple=currentTuple;
				currentTuple = child.getNextTuple();
				//System.out.println(currentTuple.getValues().values());

			}
			if (prevTuple == null)
				System.out.println("this is null");
			System.out.println(prevTuple.getTable());
			//System.out.println(currentTuple.getTable());
			if(currentTuple.getTable().equals("ENDOFFILE"))
			{
				Tuple garbo = new Tuple();
				garbo.setName("ENDOFFILE");
				return garbo;

			}

			boolean equality = true;
			
			System.out.println(prevTuple.getValues().size());
			for(int i =0; i< prevTuple.getValues().size(); i++)
			{
				System.out.println("got iside this for loop");
				String key = prevTuple.getValues().keySet().toArray()[i].toString();
				if(!(prevTuple.getValues().get(key).toString().equals(currentTuple.getValues().get(key).toString())))
				{
					equality = false;
					break;
				}
			}
			if (equality)
			{
				getNextTuple();
			}
			else 
		    {
				System.out.println("value of currenttuple is " + currentTuple.getValues().values());
				return currentTuple;
			}
		}
		//
		if (sortType == 1 && child instanceof ExternalSortOperator) {
			if (prevTuple == null) {
				prevTuple = child.getNextTuple();
				currentTuple = prevTuple;
				return prevTuple;
			}
			else {

				prevTuple=currentTuple;
				currentTuple = child.getNextTuple();
				//System.out.println(currentTuple.getValues().values());

			}
			if (prevTuple == null)
				System.out.println("this is null");
			System.out.println(prevTuple.getTable());
			//System.out.println(currentTuple.getTable());
			if(currentTuple.getTable().equals("ENDOFFILE"))
			{
				Tuple garbo = new Tuple();
				garbo.setName("ENDOFFILE");
				return garbo;

			}

			boolean equality = true;
			
			System.out.println(prevTuple.getValues().size());
			for(int i =0; i< prevTuple.getValues().size(); i++)
			{
				System.out.println("got iside this for loop");
				String key = prevTuple.getValues().keySet().toArray()[i].toString();
				if(!(prevTuple.getValues().get(key).toString().equals(currentTuple.getValues().get(key).toString())))
				{
					equality = false;
					break;
				}
			}
			if (equality)
			{
				getNextTuple();
			}
			else 
		    {
				System.out.println("value of currenttuple is " + currentTuple.getValues().values());
				return currentTuple;
			}
		}

		else
		{	
			System.out.println("why are we here");
			Tuple receivedTuple = child.getNextTuple();
			while(receivedTuple==null)
				receivedTuple=child.getNextTuple();
			if(receivedTuple.getTable().equals("ENDOFFILE"))
			{
				Tuple garbo = new Tuple();
				garbo.setName("ENDOFFILE");
				return garbo;

			}
			else
			{
				tupleValueString = tupleToString(receivedTuple);
				if(tupleList.size() == 0)
				{
					tupleList.add(tupleValueString);
					return receivedTuple;
					
				}
				else
				{
					for(int i =0; i< tupleList.size(); i++)
					{
						String a = tupleList.get(i);
						if(a.equals(tupleValueString))
						{
							return getNextTuple();
						}
					}
					
						tupleList.add(tupleValueString);
						return receivedTuple;
					
				}
				
			}
		}
		return currentTuple;

	}

	//@Override
	public void dump() throws IOException {
		// TODO Auto-generated method stub\

		Tuple a = getNextTuple();
		
		if (sortType == 0)
			columnList = ((SortOperator) child).getColumnList();
		else
			columnList = ((ExternalSortOperator) child).getColumnList();

		while(true)
		{
			if(a.getTable()!=null && a.getTable().equals("ENDOFFILE"))
			{
				System.out.println("endoffile has happened");
				writer.write();
				break;
			}
			else
			{
				if(!(items.get(0) instanceof AllColumns)){
					ArrayList<String> itemList = new ArrayList<String>();
				for(int i =0; i<items.size()-1; i++)
				{
					String colName = ((Column)((SelectExpressionItem) items.get(i)).getExpression()).toString();
					  itemList.add(colName);
					System.out.print(a.getValues().get(colName) + ", ");

				}
				String colName = ((Column)((SelectExpressionItem) items.get(items.size()-1)).getExpression()).toString();
				itemList.add(colName);
				System.out.println(a.getValues().get(colName));
				a.setOutputOrder(itemList);
				writer.writeTuple(a);
				a=getNextTuple();
				}
				else
				{
				for(int i =0; i< columnList.size()-1; i++)
				{
					String colName =  columnList.get(i);
					System.out.print(a.getValues().get(colName) + ", ");
				}
				String colName = columnList.get(columnList.size()-1);
				System.out.println(a.getValues().get(colName));
				a.setOutputOrder(columnList);
				writer.writeTuple(a);
				a=getNextTuple();
				}
			}
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

}
