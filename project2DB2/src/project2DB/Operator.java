package project2DB;

import java.io.IOException;

/**
 * Operator is an abstract class containing the blueprints for all Operator classes.
 * It has an abstract getNextTuple, dump, and reset
 * 
 * @author Robert Cao rrc8 Pulkit Kashyap pk374 Jason Zhou jz629
 */
public abstract  class Operator {
	public Operator child = null;
	
	/*
	 * GetNextTuple
	 * Dump
	 * setChild?
	 * reset
	 * getChild
	 * kid variable (general operator is sufficient)
	 */
    public abstract Tuple getNextTuple();
    
    /**
     * Reset resets this operator so that getNextTuple() will return the first tuple of the 
     * operator as if a new one was created.
     */
    public abstract void reset();
    
    /**
     * Sets the child of this operator to kid
     * 
     * @param kid
     */
    public  void setChild(Operator kid){
    	child = kid;
    }
    
    /**
     * Gets the child of this operator
     * 
     * @return child
     * 			the child of this operator
     */
    public  Operator getChild(){
    	return child;
    }
    
    /**
     * Reset resets this operator so that getNextTuple() will return the tuple at the index of the 
     * operator as if a new one was created.
     * 
     * @param index
     */
    public abstract void reset(int index);
    
    /**
     * Gets every single tuple of the operator. 
     * @throws IOException
     */
    public abstract void dump() throws IOException;
   
}
