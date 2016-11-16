package project2DB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Stack;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.schema.Table;


/**
 * JoinTreeVisitor is the class that given the where statement of a JOIN, will compute the logic for 
 * deciding if an expression is a single table selection or a join.
 * Further logic will be discussed in the README.
 * 
 * @author Robert Cao rrc8 Pulkit Kashyap pk374
 */
public class JoinTreeVisitor implements ExpressionVisitor {

	private Stack<Table> tableStack = new Stack<Table>();
	private HashMap<String, Expression> soloMap;
	private HashMap<String, Expression> joinMap;
	private ArrayList<Expression> rejectedJoins;
	private Hashtable<String, String> tableAlias = new Hashtable<String, String>();
	private String doNotJoin;

	public JoinTreeVisitor(Table tableN, Hashtable<String, String> tableNames) {
		soloMap = new HashMap<String, Expression>();
		joinMap = new HashMap<String, Expression>();
		rejectedJoins = new ArrayList<Expression>();
		tableStack = new Stack<Table>();
		tableAlias = tableNames;
		if(tableN.getAlias()!=null){
		doNotJoin = tableN.getAlias();
		}
		else{
			doNotJoin=tableN.toString();
		}
	}

	/**
	 * Returns the HashMap containing all of the single table expressions
	 * 
	 * @return soloMap
	 *            the HashMap containing the single table expressions
	 */
	public HashMap<String, Expression> getSoloMap()
	{
		return soloMap;
	}

	/**
	 * Returns the HashMap containing all of the join table expressions
	 * 
	 * @return joinMap
	 *            the HashMap containing the join table expressions
	 */
	public HashMap<String, Expression> getJoinMap() 
	{
		return joinMap;
	}

	
	/**
	 * Returns the ArrayList containing all of the unmatched join expressions
	 * 
	 * @return rejectedJoins
	 *           ArrayList containing all of the unmatched join expressions
	 */
	public ArrayList<Expression> getRejectedJoins()
	{
		return rejectedJoins;
	}

	/**
	 * Given an expression, decides whether it should be a solo or a join expression, as well as which table(s)
	 * the expression is referring to.
	 * 
	 * @param e
	 *            Expression to be passed in
	 */
	public void expressionHandler(BinaryExpression e)
	{
		e.getLeftExpression().accept(this);

		e.getRightExpression().accept(this);

		Table right1 = tableStack.pop();
		Table left1 = tableStack.pop();
		String right="";
		String left="";
		
		if(right1!=null)
		 right = tableAlias.get(right1.toString());
		if(left1!=null)
			left = tableAlias.get(left1.toString());
		
		if (left1.toString().equals(right.toString()) || left == "") 
		{
			if(soloMap.get(right)!=null)
			{
				Expression contained = soloMap.remove(right);
				AndExpression newAnd = new AndExpression();
				newAnd.setLeftExpression(contained);
				newAnd.setRightExpression(e);
				soloMap.put(right,newAnd);
			}
			else
			{			

				soloMap.put(right, e);
			}

		}
		else if (right == "")
		{	
			if(soloMap.get(left)!=null)
			{
				Expression contained = soloMap.remove(left);
				AndExpression newAnd = new AndExpression();
				newAnd.setLeftExpression(contained);
				newAnd.setRightExpression(e);
				soloMap.put(left,newAnd);
			}
			else
			{
			soloMap.put(left, e);
			}
		}
		else {
			
			if (joinMap.containsKey(left)  && joinMap.containsKey(right)) {
				rejectedJoins.add(e);
			}
			else if (!joinMap.containsKey(left) && !joinMap.containsKey(right))
			{
				if(left1.toString().equals(doNotJoin)){
					joinMap.put(right, e);


				}
				else{
					joinMap.put(left, e);
 
				}
			}
			else if (joinMap.containsKey(left) && !(right1.toString().equals(doNotJoin)) )
			{
				joinMap.put(right, e);
			}
			else if (joinMap.containsKey(right) && !(left1.toString().equals(doNotJoin)))
				joinMap.put(left, e);
			else if(joinMap.containsKey(left) && right1.toString().equals(doNotJoin))
			{
				Expression r = joinMap.remove(left);
				joinMap.put(left, e);
				rejectedJoins.add(r);
			}
			else if(joinMap.containsKey(right) && left1.toString().equals(doNotJoin))
			{
				Expression r = joinMap.remove(right);
				joinMap.put(right, e);
				rejectedJoins.add(r);
			}
			else
				rejectedJoins.add(e);
		}
	}
	public void visit(Addition e) {

	}

	public void visit(AllComparisonExpression e) {

	}

	
	/**
	 * Method for visiting an AndExpression
	 * 
	 * @param e
	 *           AndExpression to be broken down into single and join expressions
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
	 * Method for visiting a Column
	 * 
	 * @param e
	 *          Column to be visited
	 */
	public void visit (Column e) {
		//get column Name
	
		tableStack.push(e.getTable());
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
	 * Method for visiting an EqualsTo
	 * 
	 * @param e
	 *           EqualsTo to be broken down into single and join expressions
	 */
	public void visit(EqualsTo e) {
		expressionHandler(e);
	}

	public void visit(ExistsExpression e) {

	}

	public void visit(Function e) {

	}

	/**
	 * Method for visiting a GreaterThan
	 * 
	 * @param e
	 *           GreaterThan to be broken down into single and join expressions
	 */
	public void visit(GreaterThan e) {
		expressionHandler(e);
	}

	/**
	 * Method for visiting a GreaterThanEquals
	 * 
	 * @param e
	 *           GreaterThanEquals to be broken down into single and join expressions
	 */
	public void visit(GreaterThanEquals e) {
		expressionHandler(e);
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
	 * Method for visiting a LongValue
	 * 
	 * @param e
	 *          LongValue to be visited
	 */
	public void visit(LongValue e) {
		tableStack.push(null);

	}

	public void visit(Matches e) {

	}

	/**
	 * Method for visiting a MinorThan
	 * 
	 * @param e
	 *           MinorThan to be broken down into single and join expressions
	 */
	public void visit(MinorThan e) {
		expressionHandler(e);
	}

	/**
	 * Method for visiting a MinorThenEquals
	 * 
	 * @param e
	 *           MinorThanEquals to be broken down into single and join expressions
	 */
	public void visit(MinorThanEquals e) {
		expressionHandler(e);
	}

	public void visit(Multiplication e) {

	}

	/**
	 * Method for visiting a NotEqualsTo
	 * 
	 * @param e
	 *           NotEqualsTo to be broken down into single and join expressions
	 */
	public void visit(NotEqualsTo e) {
		expressionHandler(e);

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
