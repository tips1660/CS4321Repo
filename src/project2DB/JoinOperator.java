package project2DB;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;


/**
 * JoinOperator is the class representing the join operator.
 * 
 * @author Robert Cao rrc8 Pulkit Kashyap pk374
 */
public class JoinOperator extends Operator {
	private Operator leftOp;
	private Operator rightOp;

	private Expression rightExp;
	private Expression joinExp;
	private ExpressionTester testVisitor = new ExpressionTester();
	Tuple a;
	Tuple b;
	Tuple returnedTuple;
	int ctr=0;
	private boolean endoffile = false;

	private HashMap<String, Expression> soloMap = new HashMap<String, Expression>();
	private HashMap<String, Expression> joinMap  = new HashMap<String, Expression>(); 
	private ArrayList<Expression> rejectedJoins = new ArrayList<Expression>();
	private List<Join> joinList;
	private int u;
	private IndexVisitor visitor = new IndexVisitor();
	
	public JoinOperator(Operator lOp, List<Join> jList, HashMap<String, Expression> solo, HashMap<String, Expression> join, ArrayList<Expression> rejected, int useIndexes) throws FileNotFoundException {
		joinList = jList;

		leftOp = lOp;
		soloMap = solo;
		joinMap = join;
		rejectedJoins = rejected;
		u = useIndexes;

		rightExp = soloMap.get(((Table)joinList.get(0).getRightItem()).getWholeTableName());
		if (rightExp == null){
			rightOp = new scanOperator(((Table)joinList.get(0).getRightItem()));
		}
		else{
			if (u == 0)
				rightOp = new SelectOperator(((Table)joinList.get(0).getRightItem()), rightExp);
			else {
				String t = ((Table)joinList.get(0).getRightItem()).getWholeTableName();
				String index = visitor.setIndex(t);
				rightExp.accept(visitor);
				visitor.buildExpressions();
				boolean newChild = false;
				boolean fullIndex = false;
				IndexScanOperator op = null;
				if (!visitor.getIndexArray().isEmpty()) {
					newChild = true;
					int cluster = visitor.setCluster(index);
					File f = visitor.getFile();
					try {
						op = new IndexScanOperator((Table)joinList.get(0).getRightItem(), f, index, cluster, visitor.getLow(), visitor.getHigh());
					} catch (IOException e) {
						e.printStackTrace();
					}
					if (visitor.getNotIndexExpression() != null) {
						rightExp = visitor.getNotIndexExpression();
					}
					else {
						newChild = false;
						fullIndex = true;
						rightOp = op;
					}
				}
				if (!fullIndex) {
					rightOp = new SelectOperator((Table)joinList.get(0).getRightItem(), rightExp);
				}
				if (newChild) {
					rightOp.setChild(op);
				}
			}
		}

		joinExp = joinMap.get(((Table)joinList.get(0).getRightItem()).getWholeTableName());
		// would want a new join operator with the current stuff removed
		if (joinExp == null) {

			if (!rejectedJoins.isEmpty()) {
				joinExp = (BinaryExpression) rejectedJoins.get(0);

				rejectedJoins.remove(0);
			}

		}

		while(a==null) {
			a= leftOp.getNextTuple();
		}
		System.out.println("joincode: " + a.getValues());
		b=rightOp.getNextTuple();
		if(b!=null)
			System.out.println("joincode: " + b.getValues());
	}

	/**
	 * getNextTuple() returns the next tuple of this join operator, or an ENDOFFILE object if there are no more tuples
	 * 
	 * @returns returnedTuple
	 */
	@Override
	public Tuple getNextTuple() {

		while(b==null)
			b=rightOp.getNextTuple();

		while(!(b.getTable().equals("ENDOFFILE"))  && !(a.getTable().equals("ENDOFFILE")))
		{	 

			returnedTuple = new Tuple();

			returnedTuple.setName(a.getTable());
			Hashtable<String, Integer> vals = concatTuples(a,b);
			returnedTuple.setValues(vals);
			testVisitor.setTuple(returnedTuple);
			if(joinExp !=null)
			{
				joinExp.accept(testVisitor);
				
				b=rightOp.getNextTuple();
				while(b==null)
					b=rightOp.getNextTuple();
				
				if(testVisitor.getResult() == true){
					return returnedTuple;
				}
				else{
					return null;
				}
			}
			else
			{
				b= rightOp.getNextTuple();

				while(b==null)
					b=rightOp.getNextTuple();
				return returnedTuple;
			}
		}

		if(!(a.getTable().equals("ENDOFFILE")) && b.getTable().equals("ENDOFFILE"))
		{  
			rightOp.reset();
			a=leftOp.getNextTuple();

			while(a==null){
				a = leftOp.getNextTuple();
			}

			b=rightOp.getNextTuple();

			return getNextTuple();

		}
		else if(a.getTable().equals("ENDOFFILE"))
		{
			Tuple s = new Tuple();
			s.setName("ENDOFFILE");
			return s;
		}


		a=leftOp.getNextTuple();
		b = rightOp.getNextTuple();
		if(a!=null){
			returnedTuple.setValues(concatTuples(a,b));
			ctr++;

			return returnedTuple;
		}
		else{
			return null;
		}

	}
	
	
	/**
	 * For testing purposes
	 */
	//@Override
	public void dump() {
		
		System.out.println(leftOp);
		System.out.println(rightOp);
		System.out.println(rightExp);
		System.out.println(joinExp);

	}

    /**
     * Resets the Join Operator
     */
	@Override
	public void reset() {
		leftOp.reset();
		rightOp.reset();
		a = leftOp.getNextTuple();
		b=rightOp.getNextTuple();

	}
    /**
     * @param k
     * Sets the Left Operator to k
     */
	public void setLeftOperator(Operator k)
	{
		leftOp= k;
	}
	/**
     * @param k
     * Sets the right Operator to k
     */
	public void setRightOperator(Operator k)
	{
		rightOp = k;
	}
	/**
	 * 
	 * @return Operator --> gets Left Operator
	 */
	public Operator getLeftOperator()
	{
		return leftOp;
	}
	/**
	 * 
	 * @return Operator --> gets right Operator
	 */
	public Operator getRightOperator()
	{
		return rightOp;
	}

	 /**
	  * 
	  * @param tuple1
	  * @param tuple2
	  * @return Hashtable
	  *  Gets a hashtable with concatenated tuples
	  */
	public Hashtable<String, Integer> concatTuples(Tuple tuple1, Tuple tuple2)
	{
		Hashtable<String, Integer> val1 = new Hashtable<String, Integer>();
		for(int i =0; i< tuple1.getValues().size(); i++)
		{
			String accessor = (String)(tuple1.getValues().keySet().toArray()[i]);
			val1.put(accessor,(Integer)tuple1.getValues().get(accessor));

		}

		Hashtable<String, Integer> val2= tuple2.getValues();
		for(int i =0; i < val2.size(); i++)
		{
			String accessor = (String)(val2.keySet().toArray()[i]);
			//gets the key im on for tuple2
			if(val1.get(accessor) !=null)
			{
				val1.put(accessor + "1", val2.get(accessor));
			}
			else
			{
				
				val1.put(accessor,val2.get(accessor));

			}
		}
		return val1;
	}
	
	@Override
	public void reset(int index) {
		// TODO Auto-generated method stub
		
	}
}
	