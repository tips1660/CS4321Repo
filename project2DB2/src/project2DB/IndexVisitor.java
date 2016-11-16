package project2DB;

import java.io.File;
import java.util.ArrayList;
import java.util.Stack;

import net.sf.jsqlparser.expression.*;
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
public class IndexVisitor implements ExpressionVisitor {


	private Stack<Integer> valueStack = new Stack<Integer>();

	private int high = -1;
	private int low = -1;
	
	private Stack<String> columnStack = new Stack<String>();
	
	private DatabaseCatalog dbcat = DatabaseCatalog.getInstance();
	private ArrayList<Expression> indexExp = new ArrayList<Expression>();
	private ArrayList<Expression> notIndexExp = new ArrayList<Expression>();
	private Expression indexExpression = null;
	private Expression notIndexExpression = null;
	private String indexName = null;
	private int cluster;
	private File f = null;
	private boolean isIndex = true;
	public IndexVisitor () 
	{
		
	}
	
	public String setIndex(String s)
	{
		indexName = dbcat.getIndex(s);
		if (indexName == null) {
			indexName = "";
			isIndex = false;
		}
		else {
			setFile();
		}
		return indexName;
	}
	
	public int setCluster(String s)
	{
		cluster = dbcat.getCluster(s);
		return cluster;
	}
	
	public void setFile()
	{
		if (!indexName.equals(""))
			f = dbcat.getFile(indexName);
	}
	
	public boolean doesIndexExist()
	{
		return isIndex;
	}
	
	public void reset()
	{
		indexExpression = null;
		notIndexExpression = null;
		indexExp.clear();
		notIndexExp.clear();
	}
	
	public File getFile()
	{
		return f;
	}
	
	public ArrayList<Expression> getIndexArray() 
	{
		return indexExp;
	}
	
	public int getHigh()
	{
		return high;
	}
	
	public int getLow()
	{
		return low;
	}
	
	public Expression getIndexExpression() {
		return indexExpression;
	}
	
	public Expression getNotIndexExpression() {
		return notIndexExpression;
	}
	
	public void buildExpressions() {
	
		/*
		if (indexExp.size() >= 2) {
			indexExpression = new AndExpression();
			((BinaryExpression) indexExpression).setLeftExpression(indexExp.get(0));
			((BinaryExpression) indexExpression).setRightExpression(indexExp.get(1));
			for (int i = 2; i < indexExp.size(); i++) {
				((BinaryExpression) indexExpression).setLeftExpression(indexExpression);
				((BinaryExpression) indexExpression).setRightExpression(indexExp.get(i));
			}
		}
		else if (!indexExp.isEmpty()) {
			indexExpression = indexExp.get(0);
		}
		*/
		if (notIndexExp.size() >= 2) {
			notIndexExpression = new AndExpression();
			((BinaryExpression) notIndexExpression).setLeftExpression(notIndexExp.get(0));
			((BinaryExpression) notIndexExpression).setRightExpression(notIndexExp.get(1));
			for (int i = 2; i < notIndexExp.size(); i++) {
				((BinaryExpression) notIndexExpression).setLeftExpression(notIndexExpression);
				((BinaryExpression) notIndexExpression).setRightExpression(notIndexExp.get(i));
			}
		}
		else if (!notIndexExp.isEmpty()) {
			notIndexExpression = notIndexExp.get(0);
		}
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
		e.getRightExpression().accept(this);
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
		//Integer k = (Integer)t.getValues().get(e.toString());
		valueStack.push(-1);
        //valueStack.push(k); 
        columnStack.push(e.toString());
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
	    String x = columnStack.pop();
	    int right = valueStack.pop();
	    String y = columnStack.pop();
	    int left = valueStack.pop();
	    //check if index works
	    if (!x.equals("NUMBER") && !y.equals("NUMBER"))
	    	notIndexExp.add(e);
	    else {
	    	boolean h = (indexName.equals(x)) || (indexName.equals(y));
	    	if (h) {
	    		if (x.equals("NUMBER")) {
	    			high = right;
	    			low = right;
	    		}
	    		else {
	    			high = left;
	    			low = left;
	    		}
	    		indexExp.add(e);
	    	}
	    	else {
	    		notIndexExp.add(e);
	    	}
	    }
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
	    String x = columnStack.pop();
	    int right = valueStack.pop();
	    String y = columnStack.pop();
	    int left = valueStack.pop();
	    //check if index works
	    if (!x.equals("NUMBER") && !y.equals("NUMBER"))
	    	notIndexExp.add(e);
	    else {
	    	boolean h = (indexName.equals(x)) || (indexName.equals(y));
	    	if (h) {
	    		if (x.equals("NUMBER")) {
	    			if (high == low && high < 0) {
	    				low = right + 1;
	    				high = -1;
	    			}
	    			else if (high != low) {
	    				low = right + 1;
	    			}
	    		}
	    		else {
	    			if (high == low && high < 0) {
	    				low = -1;
	    				high = left - 1;
	    			}
	    			else if (high != low) {
	    				high = left - 1;
	    			}
	    		}
	    		indexExp.add(e);
	    	}
	    	else {
	    		notIndexExp.add(e);
	    	}
	    }
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
	    String x = columnStack.pop();
	    int right = valueStack.pop();
	    String y = columnStack.pop();
	    int left = valueStack.pop();
	    //check if index works
	    if (!x.equals("NUMBER") && !y.equals("NUMBER"))
	    	notIndexExp.add(e);
	    else {
	    	boolean h = (indexName.equals(x)) || (indexName.equals(y));
	    	if (h) {
	    		if (x.equals("NUMBER")) {
	    			if (high == low && high < 0) {
	    				low = right;
	    				high = -1;
	    			}
	    			else if (high != low) {
	    				low = right;
	    			}
	    		}
	    		else {
	    			if (high == low && high < 0) {
	    				low = -1;
	    				high = left;
	    			}
	    			else if (high != low) {
	    				high = left;
	    			}
	    		}
	    		indexExp.add(e);
	    	}
	    	else {
	    		notIndexExp.add(e);
	    	}
	    }
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
		columnStack.push("NUMBER");
		
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
	    String x = columnStack.pop();
	    int right = valueStack.pop();
	    String y = columnStack.pop();
	    int left = valueStack.pop();
	    //check if index works
	    if (!x.equals("NUMBER") && !y.equals("NUMBER"))
	    	notIndexExp.add(e);
	    else {
	    	boolean h = (indexName.equals(x)) || (indexName.equals(y));
	    	if (h) {
	    		if (x.equals("NUMBER")) {
	    			if (high == low && high < 0) {
	    				low = -1;
	    				high = right - 1;
	    			}
	    			else if (high != low) {
	    				high = right - 1;
	    			}
	    		}
	    		else {
	    			if (high == low && high < 0) {
	    				low = left + 1;
	    				high = -1;
	    			}
	    			else if (high != low) {
	    				low = left + 1;
	    			}
	    		}

	    		indexExp.add(e);
	    	}
	    	else {
	    		notIndexExp.add(e);
	    	}
	    }
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
	    String x = columnStack.pop();
	    int right = valueStack.pop();
	    String y = columnStack.pop();
	    int left = valueStack.pop();
	    //check if index works
	    if (!x.equals("NUMBER") && !y.equals("NUMBER"))
	    	notIndexExp.add(e);
	    else {
	    	boolean h = (indexName.equals(x)) || (indexName.equals(y));
	    	if (h) {
	    		if (x.equals("NUMBER")) {
	    			if (high == low && high < 0) {
	    				low = -1;
	    				high = right;
	    			}
	    			else if (high != low) {
	    				high = right;
	    			}
	    		}
	    		else {
	    			if (high == low && high < 0) {
	    				low = left;
	    				high = -1;
	    			}
	    			else if (high != low) {
	    				low = left;
	    			}
	    		}

	    		indexExp.add(e);
	    	}
	    	else {
	    		notIndexExp.add(e);
	    	}
	    }
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
		notIndexExp.add(e);
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
