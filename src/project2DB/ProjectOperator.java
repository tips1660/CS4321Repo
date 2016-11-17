package project2DB;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;


/**
 * ProjectOperator is the class representing the project operator.
 * 
 * @author Robert Cao rrc8 Pulkit Kashyap pk374
 */
public class ProjectOperator extends Operator {
	/*
	 * 
	 */
	private ArrayList<SelectItem> items;
	Table table;
	tableProperties tableR;
	tableProperties tableY;
	private DatabaseCatalog dbcat = DatabaseCatalog.getInstance();
	String fileUrl;
	Scanner scan;
	String tableName;
	ArrayList<String> columnList = new ArrayList<String>();
	private Hashtable<String, String> tableAlias = new Hashtable<String, String>();
	private TupleWriter writer;
	AllColumns asterik = new AllColumns();


	public ProjectOperator(String out, ArrayList<SelectItem> items, List<Join> joinList, Table tableN) throws IOException{
		/*
		 * So make either a join, if not that a select, if not that a scan
		 */

		tableName = tableN.getWholeTableName();
		table = tableN;
		System.out.println("writing");
		writer = new TupleWriter(out);
		System.out.println("done writing");

		tableR = (tableProperties) dbcat.getTableCatalog().get(tableName);
		for(int i =0; i<tableR.getColumns().size(); i++)
		{
			if(tableN.getAlias()!=null)
				columnList.add(tableN.getAlias() + "." +tableR.getColumns().get(i));
			else{
				if(tableN.getWholeTableName().substring(0, 1).equals("."))
				{
					System.out.println("I am coming to this thing");
					columnList.add(tableN.getWholeTableName().substring(1) + "." + tableR.getColumns().get(i));
				}
				else{

					columnList.add(tableN.getWholeTableName() + "." + tableR.getColumns().get(i));
				}
			}
		}

		if(joinList!=null){
			for(int i =0; i <joinList.size(); i++)
			{
				Table tempTable = (Table) joinList.get(i).getRightItem();
				String tableTemp =  tempTable.getWholeTableName();
				String aliasTemp = tempTable.getAlias();
				tableY = (tableProperties) dbcat.getTableCatalog().get(tableTemp);

				for(int j =0; j<tableY.getColumns().size(); j++)
				{
					if(aliasTemp!=null)
					{
						columnList.add(aliasTemp+"."+tableY.getColumns().get(j));
					}
					else
					{
						columnList.add(tableTemp+"."+tableY.getColumns().get(j));
					}
				}
			}
			//child=new JoinOperatorSuper(tableN, joinList, select);

		}
		//else if (select != null) {
		//child = new SelectOperator(tableN, select);

		//}
		//else {
		//child = new scanOperator(tableN);

		//}

		if(items!=null){
			this.items = items;
			}
			else{
				this.items = new ArrayList<SelectItem>();
				this.items.add(asterik);
			}

		fileUrl = tableR.getUrl();

		File f = new File(fileUrl);
		scan = new Scanner(f);

	}

	public ProjectOperator(ArrayList<SelectItem> items, List<Join> joinList,  Table tableN) throws IOException{
		/*
		 * So make either a join, if not that a select, if not that a scan
		 */

		tableName = tableN.getWholeTableName();
		if(tableName.substring(0, 1).equals("."))
		{
			tableName = tableName.substring(1);
		}
		table = tableN;

		//writer = null;

		tableR = (tableProperties) dbcat.getTableCatalog().get(tableName);


		for(int i =0; i<tableR.getColumns().size(); i++)
		{
			if(tableN.getAlias()!=null){
				columnList.add(tableN.getAlias() + "." +tableR.getColumns().get(i));
			}
			else{
				if(tableN.getWholeTableName().substring(0, 1).equals("."))
				{
					System.out.println("I am coming to this thing");
					columnList.add(tableN.getWholeTableName().substring(1) + "." + tableR.getColumns().get(i));
				}
				else{

					columnList.add(tableN.getWholeTableName() + "." + tableR.getColumns().get(i));
				}			
			}
			
		}
		

		if(joinList!=null){
			for(int i =0; i <joinList.size(); i++)
			{
				Table tempTable = (Table) joinList.get(i).getRightItem();
				String tableTemp =  tempTable.getWholeTableName();
				String aliasTemp = tempTable.getAlias();
				tableY = (tableProperties) dbcat.getTableCatalog().get(tableTemp);

				for(int j =0; j<tableY.getColumns().size(); j++)
				{
					if(aliasTemp!=null)
					{
						columnList.add(aliasTemp+"."+tableY.getColumns().get(j));
					}
					else
					{
						columnList.add(tableTemp+"."+tableY.getColumns().get(j));
					}
				}
			}
			//child=new JoinOperatorSuper(tableN, joinList, select);

		}
		//	else if (select != null) {
		//	child = new SelectOperator(tableN, select);

		//}
		//else {
		//child = new scanOperator(tableN);

		//}
		if(items!=null){
		this.items = items;
		}
		else{
			this.items = new ArrayList<SelectItem>();
			this.items.add(asterik);
		}



		fileUrl = tableR.getUrl();

		File f = new File(fileUrl);
		scan = new Scanner(f);

	}

	/**
	 * This method returns the list of columns in order, that will be projected.
	 * 
	 * @returns columnList
	 */
	public ArrayList<String> getColumnList()
	{
		return columnList;
	}

	/**
	 * getNextTuple() returns the next tuple of this scan operator, or an ENDOFFILE object if there are no more tuples
	 * @returns returnedTuple
	 */
	@Override
	public Tuple getNextTuple() 
	{	
		if (child instanceof SelectOperator)
			System.out.println("selectoperator");
		Tuple returnedTuple = child.getNextTuple();
		if(items == null)
			System.out.println("got it");
		if(items.get(0) instanceof AllColumns)
		{
			System.out.println("i should be going to this no2w");
			return returnedTuple;
		}
		else if(returnedTuple !=null)
		{
			if(returnedTuple.getTable().equals("ENDOFFILE"))
			{
				System.out.println("i should be going to this now");
				Tuple finalTuple = new Tuple();
				finalTuple.setName("ENDOFFILE");
				return finalTuple;
			}
			else
			{
				System.out.println("do i get to this?");
				Tuple finalTuple = new Tuple();
				Hashtable<String, Integer> tupleSet = new Hashtable<String, Integer>();

				for(int i =0; i< items.size(); i++)
				{
					String colName = ((Column)((SelectExpressionItem) items.get(i)).getExpression()).toString();
					int x = (int) returnedTuple.getValues().get(colName);

					tupleSet.put(colName, x);
				}
				if(returnedTuple.getTable()!=null)
					finalTuple.setName(returnedTuple.getTable());
				finalTuple.setValues(tupleSet);

				return finalTuple;
			}
		}
		else
		{  			
			return null;
		}
	}

	/**
	 * Dump prints out every tuple from the scan
	 * @throws IOException 
	 */
	//@Override
	public void dump() throws IOException
	{
		Tuple returnedTuple = getNextTuple();
		while(true)
		{  			
			if(returnedTuple == null)
			{

				returnedTuple = getNextTuple();
			}
			else{
				if(returnedTuple.getTable()!=null && returnedTuple.getTable().equals("ENDOFFILE"))
				{
					System.out.println("endoffile has happened");
					writer.write();
					break;
				}
				else
				{	
					System.out.println("item is null");


					if(!(items.get(0) instanceof AllColumns)){
						ArrayList<String> itemsActual = new ArrayList<String>();
						for(int i =0; i < items.size()-1; i++)
						{
							String colName = ((Column)((SelectExpressionItem) items.get(i)).getExpression()).toString();
							itemsActual.add(colName);
							System.out.print(returnedTuple.getValues().get(colName) + ",");
						}
						String colName = ((Column)((SelectExpressionItem) items.get(items.size()-1)).getExpression()).toString();
						itemsActual.add(colName);
						System.out.println(returnedTuple.getValues().get(colName));
						returnedTuple.setOutputOrder(itemsActual);
						writer.writeTuple(returnedTuple);
						returnedTuple=getNextTuple();
						//returnedTuple.setOutputOrder(itemsActual);
					}
					else{
						for(int i =0; i <columnList.size()-1; i++)
						{
							String colName = columnList.get(i);
							System.out.print(returnedTuple.getValues().get(colName) + ",");
						}
						String colName = columnList.get(columnList.size()-1);
						System.out.println(returnedTuple.getValues().get(colName));

						returnedTuple.setOutputOrder(columnList);
						writer.writeTuple(returnedTuple);
						returnedTuple = getNextTuple();
						//returnedTuple.setOutputOrder(columnList);
					}
				}

				//writer.writeTuple(returnedTuple);
			}
		}
	}

	/**
	 * Resets this operator such that getNextTuple() will return the first tuple it originally would.
	 */
	@Override
	public void reset() {

	}

	@Override
	public void reset(int index) {
		// TODO Auto-generated method stub

	}
}
