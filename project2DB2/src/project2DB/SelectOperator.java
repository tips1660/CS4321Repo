package project2DB;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;


/**
 * ScanOperator is the class representing the Select Operator
 * 
 * @author Robert Cao rrc8 Pulkit Kashyap pk374
 */
public class SelectOperator extends Operator {

	public String table = "";
	private DatabaseCatalog dbcat = DatabaseCatalog.getInstance();
	String fileUrl = "";
	Scanner scan;
	Tuple returnedTuple;
	Tuple returnedTuple2;
	Expression e;
	Table t;
	tableProperties tableR;
	ExpressionTester visitor = new ExpressionTester();
	int ctr =0;
	public SelectOperator(Table tableN, Expression input) throws FileNotFoundException
	{  t = tableN;
		table = tableN.getWholeTableName();
		tableR = (tableProperties) dbcat.getTableCatalog().get(table);
		fileUrl = tableR.getUrl();
		e=input;
		File f = new File(fileUrl);
		scan = new Scanner(f);
	}

	@Override
	/**
	 * getNextTuple() returns the next tuple of this select operator, or an ENDOFFILE object if there are no more tuples
	 * 
	 * @returns returnedTuple
	 */
	public Tuple getNextTuple() {
		if(child ==null)
			child = new scanOperator(t);
		returnedTuple = child.getNextTuple();
		if(returnedTuple.getTable().equals("ENDOFFILE"))
		{
			return returnedTuple;
		}
		else{
			visitor.setTuple(returnedTuple);
			e.accept(visitor);
			
			if(visitor.getResult()==true)
			{
				return returnedTuple;

			}
			else
			{
				return null;
			}
		}
	}


	/**
	 * Returns the result of the current tuple when evaluated by an expression
	 * 
	 * @returns visitor.getResult()
	 */
	public boolean getResult()
	{
		return visitor.getResult();
	}
	
	/**
	 * Dump prints out every tuple from the select Operator
	 */
	//@Override
	public void dump() {
		int ctr = 0;
		returnedTuple2 = getNextTuple();

		while(ctr!=6)
		{  
			if(returnedTuple2 !=null)
			{
				for(int i =0; i <returnedTuple2.getValues().size(); i++)
				{
					System.out.print(returnedTuple2.getValues().get(tableR.getColumns().get(i)) + ", ");
				}
				System.out.println();
				returnedTuple2 = getNextTuple();
			}
			else
			{
				ctr++;
				returnedTuple2 = getNextTuple();


			}


		}

	}

	@Override
	/**
	 * Resets this operator such that getNextTuple() will return the first tuple it originally would.
	 */
	public void reset() {
		child.reset();
	}
	
	@Override
	public void reset(int index) {
		// TODO Auto-generated method stub
		
	}
}
