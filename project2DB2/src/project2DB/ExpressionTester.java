package project2DB;

import java.util.Stack;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * ExpressionTester is a class that given an expression, will evaluate if it is true or not
 * 
 * @author Robert Cao rrc8 Pulkit Kashyap pk374
 */
public class ExpressionTester implements ExpressionVisitor {

	private Tuple t;
	private Stack<Integer> valueStack = new Stack<Integer>();
	private boolean result = false;
	private int compare = 0;

	public ExpressionTester () {
		
	}
	
	/**
	 * Method for setting a new tuple for boolean evaluation on the given condition
	 * 
	 * @param tu
	 *            tuple parameter that will be passed in for examination
	 */
	public void setTuple(Tuple tu)
	{ 
		t=tu;
	}
	
	/**
	 * Returns the boolean result of the expression
	 * 
	 * @return result
	 *            boolean evaluation of the expression passed in
	 */
	public boolean getResult()
	{
		return result;
	}
	public int getCompare()
	{
		return compare;
	}
	
	/**
	 * Resets fields; changes the result of the expression back to false and the tuple to null
	 */
	public void reset()
	{
		t = null;
		result = false;
	}

	public void visit(Addition e) {
          		
	}
	
	public void visit(AllComparisonExpression e) {
		
	}
	
	/**
	 * Method for visiting an AndExpression in postorder traversal. Checks if the left and right expressions are both true 
	 * and returns the result of that
	 * 
	 * @param e
	 *            AndExpression to be visited
	 */
	public void visit(AndExpression e) {
	   e.getLeftExpression().accept(this);
		 boolean expression1result  = result;
		e.getRightExpression().accept(this);
		 boolean expression2result = result;
		
		result = (expression1result && expression2result);
		
	}
	
	public void visit(AnyComparisonExpression e) {
		
	}
	
	public void visit(Between e) {
		
	}
	
	public void visit(BitwiseAnd e) {
		
	}
	
	public void visit(BitwiseOr e) {
		
	}
	
	public void visit(BitwiseXor e) {
		
	}
	
	public void visit (CaseExpression e) {
		
	}
	
	/**
	 * Method for visiting a Column. Gets the value of the tuple at the given column
	 * and pushes it onto the stack. 
	 * 
	 * @param e
	 *            Column to be visited
	 */
	public void visit (Column e) {
		Integer k = (Integer)t.getValues().get(e.toString());

          valueStack.push(k); 
	}
	
	public void visit (Concat e) {
		
	}
	
	public void visit (DateValue e) {
		
	}
	
	public void visit (Division e) {
		
	}
	
	public void visit (DoubleValue e) {
		
	}
	
	/**
	 * Method for visiting an EqualsTo in postorder traversal. Checks if the left and right
	 * expressions are equal to each other.
	 * 
	 * @param e
	 *            EqualsTo to be visited
	 */
	public void visit(EqualsTo e){
			 e.getLeftExpression().accept(this);
		     e.getRightExpression().accept(this);

		     int k = valueStack.pop();
		     int y = valueStack.pop();
		     result = (k==y);
		     if(y<k)
		    	 compare = -1;
		     else if(y==k)
		    	 compare = 0;
		     else
		    	 compare = 1;
		  
	}	

	public void visit(ExistsExpression e) {
		
	}
	
	public void visit(Function e) {
		
	}
	
	/**
	 * Method for visiting a GreaterThan in postorder traversal. Checks if the left expression is
	 * greater than the right expression.
	 * @param e
	 *            GreaterThan to be visited
	 */
	public void visit(GreaterThan e) {

		e.getLeftExpression().accept(this);
		e.getRightExpression().accept(this);
		int result1 = valueStack.pop();

		int result2= valueStack.pop();
		result = (result2 > result1);

	    
	     if(result2<=result1)
	    	 compare = -1;
	     else if(result2>result1)
	    	 compare = 0;
	     else
	    	 compare = 1;
	}
	
	/**
	 * Method for visiting a GreaterThanEquals in postorder traversal. Checks if the left expression is
	 * greater than or equals the right expression.
	 * @param e
	 *            GreaterThanEquals to be visited
	 */
	public void visit(GreaterThanEquals e) {
		e.getLeftExpression().accept(this);
		e.getRightExpression().accept(this);
		int result1 = valueStack.pop();
		int result2= valueStack.pop();
		result = (result2 >= result1);
		  if(result2<result1)
		    	 compare = -1;
		     else if(result2>=result1)
		    	 compare = 0;
		     else
		    	 compare = 1;
	}
	
	public void visit(InExpression e) {
		
	}
	
	public void visit(InverseExpression e) {
		
	}
	
	public void visit(IsNullExpression e) {
		
	}
	
	public void visit(JdbcParameter e) {
		
	}
	
	public void visit(LikeExpression e) {
		
	}
	
	/**
	 * Method for visiting a LongValue. Pushes the value of the expression onto the stack.
	 * @param e
	 *            LongValue to be visited
	 */
	public void visit(LongValue e) {
		int t = (int) e.getValue();
		valueStack.push(t);
		
	}
	
	public void visit(Matches e) {
		
	}
	
	/**
	 * Method for visiting a MinorThan in postorder traversal. Checks if the left expression is
	 * minor (less) than the right expression.
	 * @param e
	 *            MinorThan to be visited
	 */
	public void visit(MinorThan e) {
		e.getLeftExpression().accept(this);
		e.getRightExpression().accept(this);
		int result1 = valueStack.pop();
		int result2= valueStack.pop();
 
		result = (result2 < result1);
		  if(result2>=result1)
		    	 compare = 1;
		     else if(result2<result1)
		    	 compare = 0;
		     else
		    	 compare = 1;
		
	}
	
	/**
	 * Method for visiting a MinorThanEquals in postorder traversal. Checks if the left expression is
	 * minor than or equals the right expression.
	 * @param e
	 *            MinorThanEquals to be visited
	 */
	public void visit(MinorThanEquals e) {
		e.getLeftExpression().accept(this);
		e.getRightExpression().accept(this);
		int result1 = valueStack.pop();
		int result2= valueStack.pop();
		result = (result2 <= result1);
		  if(result2>result1)
		    	 compare = 1;
		     else if(result2<=result1)
		    	 compare = 0;
		     else
		    	 compare = 1;
	}
	
	public void visit(Multiplication e) {
		
	}
	
	/**
	 * Method for visiting a NotEqualsTo in postorder traversal. Checks if the left expression is
	 * not equal to the right expression.
	 * @param e
	 *            NotEqualsTo to be visited
	 */
	public void visit(NotEqualsTo e) {
		e.getLeftExpression().accept(this);
		e.getRightExpression().accept(this);
		int result1 = valueStack.pop();
		int result2= valueStack.pop();
		result = (result2 != result1);
		 if(result2!=result1)
	    	 compare = 0;
		 else 
			 compare =2;
	}
	
	public void visit(NullValue e) {
		
	}

	public void visit(OrExpression e) {
		
	}
	
	public void visit(Parenthesis e) {
		
	}
	
	public void visit(StringValue e) {
		
	}
	
	public void visit(SubSelect e) {
		
	}
	
	public void visit(Subtraction e) {
		
	}
	
	public void visit(TimestampValue e) {
		
	}

	public void visit(TimeValue e) {
		
	}
	
	public void visit(WhenClause e) {
		
	}
}
