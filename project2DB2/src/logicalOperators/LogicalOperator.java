package logicalOperators;

import java.io.IOException;

public abstract class LogicalOperator  {
   
	
	LogicalOperator child;
     public void setChild(LogicalOperator s)
     {  
    	child = s; 
     }
     public LogicalOperator getChild()
     {
    	 return child;
     }
    public abstract void accept(PhysicalPlanBuilder s) throws IOException;
}
