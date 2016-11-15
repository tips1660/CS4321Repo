package project2DB;

import java.io.IOException;

import net.sf.jsqlparser.schema.Table;

public abstract class SortOperatorParent extends Operator {

	public abstract void setTable(Table tableN);
		// TODO Auto-generated method stub
		
	

	public abstract void addToSortListActual(String leftJoinExpR);
	
 	
	 public abstract Tuple getNextTuple();
	    
	    /**
	     * Reset resets this operator so that getNextTuple() will return the first tuple of the 
	     * operator as if a new one was created.
	     */
	    public abstract void reset();
	    public abstract void dump() throws IOException;


		public abstract Table getTable();



		public abstract void sort() throws IOException;
			// TODO Auto-generated method stub
			
	 
		 

}
