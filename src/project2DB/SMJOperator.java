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
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SMJOperator extends Operator {
	Tuple a;
	Tuple b;
	private SortOperatorParent leftOp;
	private SortOperatorParent rightOp;
	int partitionCtr =0;
	int partitionTraverser = 0;
	private Expression rightExp;
	private Expression joinExp;
	private ExpressionTester testVisitor = new ExpressionTester();
	private int bufferCtr =0;
	private int currentTuple = 0;
	ExternalSortOperator leftOpEx;
	boolean ex;
	ExternalSortOperator rightOpEx;
	Tuple returnedTuple;
	int ctr=0;
	public boolean bufferSetup = false;
	private boolean endoffile = false;
	private Tuple rightPartitionTraverser = null;
	private HashMap<String, Expression> soloMap = new HashMap<String, Expression>();
	private HashMap<String, Expression> joinMap  = new HashMap<String, Expression>(); 
	private ArrayList<Expression> rejectedJoins = new ArrayList<Expression>();
	private List<Join> joinList;
	ArrayList<SelectItem> items1;
	private Hashtable<Integer, Tuple> tupleBuffer = new Hashtable<Integer, Tuple>();
	int u;
	IndexVisitor visitor = new IndexVisitor();

	public SMJOperator(SortOperatorParent lOp, List<Join> jList, HashMap<String, Expression> solo, HashMap<String, Expression> join, ArrayList<Expression> rejected,int j2, int ctr, String tempDir, boolean exTF, int u) throws IOException {
		joinList = jList;
		items1 = new ArrayList<SelectItem>();//just holds  a single instance of all columns since the real selection happens in the real project.
		AllColumns allcolplaceholder= new AllColumns();
		items1.add(allcolplaceholder);
		this.u = u;
		leftOp = lOp;
		soloMap = solo;
		joinMap = join;
		rejectedJoins = rejected;
		if(((Table)joinList.get(0).getRightItem()).getAlias() == null)
			joinExp = joinMap.get(((Table)joinList.get(0).getRightItem()).getWholeTableName());
		else
			joinExp = joinMap.get(((Table)joinList.get(0).getRightItem()).getAlias());

		if(exTF == false)
		{
			rightOp = new SortOperator(items1, null);
			ex = false;
		}
		else
		{
			rightOp = new ExternalSortOperator(tempDir, j2, ctr, items1, null);
			ex=true;

		}
		getReady();
		// would want a new join operator with the current stuff removed

	}




	public void getReady() throws IOException
	{
		if (joinExp == null) {

			if (!rejectedJoins.isEmpty()) {
				joinExp = (BinaryExpression) rejectedJoins.get(0);

				rejectedJoins.remove(0);
			}

		}
		System.out.println("SMJCode v2: My JoinEXP is: " + joinExp.toString());

		if(((Table)joinList.get(0).getRightItem()).getAlias() == null)
		rightExp = soloMap.get(((Table)joinList.get(0).getRightItem()).getWholeTableName());
		else
			rightExp = soloMap.get(((Table)joinList.get(0).getRightItem()).getAlias());

		if (rightExp == null){
			// for in memory sorting
			scanOperator scanRight = new scanOperator(((Table)joinList.get(0).getRightItem()));
			ProjectOperator projectRight = new ProjectOperator(items1, joinList,((Table)joinList.get(0).getRightItem()));
			projectRight.setChild(scanRight);
			rightOp.addToSortListActual(((BinaryExpression)(joinExp)).getRightExpression().toString());
			rightOp.setChild(projectRight);
			System.out.println("SMJCode v1: Made my right Op");

		}
		else{
			System.out.println("did i get into this thing? my right exp is: " + rightExp );
			Operator rightSelectOp = null;
			if (u == 0) {
				rightSelectOp = new SelectOperator(((Table)joinList.get(0).getRightItem()), rightExp);
			}
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
						rightSelectOp = op;
					}
				}
				if (!fullIndex) {
					rightSelectOp = new SelectOperator((Table)joinList.get(0).getRightItem(), rightExp);
				}
				if (newChild) {
					rightSelectOp.setChild(op);
				}
			}
			ProjectOperator projectRight  = new ProjectOperator( items1,joinList, ((Table)joinList.get(0).getRightItem()));
			projectRight.setChild(rightSelectOp);

			rightOp.addToSortListActual(((BinaryExpression)(joinExp)).getRightExpression().toString());
			rightOp.setChild(projectRight);

		}
		System.out.println("got out of this");


		while(a==null){
			a= leftOp.getNextTuple();


		}

		System.out.println("SMJCode v3: my a tuple is: " + a.getValues());

		b=rightOp.getNextTuple();

		if(b!=null)
			System.out.println("SMJCode v3: my b tuple is: " + b.getValues());
	}

	public int tupleCompare(Tuple a, Tuple b)
	{	
		if(b.getTable().equals("ENDOFFILE") || a.getTable().equals("ENDOFFILE")){
			return -1;
		}

		returnedTuple = new Tuple();
		returnedTuple.setName(a.getTable());

		Hashtable<String, Integer> vals = concatTuples(a,b);
		returnedTuple.setValues(vals);
		if(joinExp !=null)
		{
			testVisitor.setTuple(returnedTuple);
			joinExp.accept(testVisitor);
			return testVisitor.getCompare();
		}

		return 0;
	}
	public int getPartitionCtr()
	{
		return partitionCtr;
	}
	/*
	 * assumes that both the relations l and r have been sorted already
	 */
	public void bufferSetUp() {
		System.out.println("do i get into buffer set up?");
		while(b==null){
			partitionCtr++;
			b=rightOp.getNextTuple();

		}

		if(b.getValues() !=null && a.getValues()!=null){
			System.out.println("CodexB: " + b.getValues());
			System.out.println("CodexA: " + a.getValues());
		}

		while(!(b.getTable().equals("ENDOFFILE"))  && !(a.getTable().equals("ENDOFFILE")))
		{
			if(((BinaryExpression)(joinExp)).getRightExpression().toString().equals("Boats.D"))
			{
				System.out.println("the values from my boat tuple are as follow: " + b.getValues());
				System.out.println("The value from my original thing is as follows: "  + a.getValues());
			}
			System.out.println("I got into the while loop for my buffersetup");
			int compareVal = tupleCompare(a,b);
			while(!(b.getTable().equals("ENDOFFILE")) && !(a.getTable().equals("ENDOFFILE")) &&tupleCompare(a,b) !=0){
				while(tupleCompare(a,b) == 1)
				{

					b=rightOp.getNextTuple();

					partitionCtr++;
					if(!b.getTable().equals("ENDOFFILE"))
						continue;
					else
						break;
				}
				while(tupleCompare(a,b)==-1)
				{	

					a=leftOp.getNextTuple();

					if(!a.getTable().equals("ENDOFFILE"))
						continue;
					else
						break;
				}
				while(tupleCompare(a,b) == 2)
				{
					a=leftOp.getNextTuple();

					if(!a.getTable().equals("ENDOFFILE")){

						b= rightOp.getNextTuple();

						if(!b.getTable().equals("ENDOFFILE"))
							continue;
						else
							break;
					}
					else
					{
						break;
					}

				}
			}
			rightPartitionTraverser = b;
			int beforeLoop = partitionCtr;

			while(tupleCompare(a,b) == 0)
			{
				System.out.println("should have come into this  while loop");
				System.out.println("SMJCode v4: my A is: " + a.getValues() + " My b is:  " + b.getValues());
				/*
				 * need to add the code for resetting.
				 */
				partitionTraverser=0;
				System.out.println(partitionCtr + "my partition Ctr is this value..");
				rightOp.reset(partitionCtr);
				rightPartitionTraverser=rightOp.getNextTuple();

				System.out.println("My right partition is at: " + rightPartitionTraverser.getValues());

				if(rightPartitionTraverser == b)
					System.out.println("partitionThing and b are equal");

				while (tupleCompare(a, rightPartitionTraverser) == 0)
				{
 					returnedTuple = new Tuple();
					Hashtable<String, Integer> values = new Hashtable<String, Integer>();
					values = concatTuples(a,rightPartitionTraverser);
					returnedTuple.setName(a.getTable());
					returnedTuple.setValues(values);
					tupleBuffer.put(bufferCtr, returnedTuple);
					bufferCtr++;
					System.out.println(returnedTuple.getValues());
					rightPartitionTraverser = rightOp.getNextTuple();


					partitionTraverser++;
					while(rightPartitionTraverser == null){
						rightPartitionTraverser = rightOp.getNextTuple();

						partitionTraverser++;
					}
				}

				a =leftOp.getNextTuple();

				while(a==null)
				{   

					a=leftOp.getNextTuple();


				}
				if(a.getValues()!=null)
					System.out.println("RB: moving my A up, still with the same B, new A has the followign values: " + a.getValues());
			}
			System.out.println(b.getValues());
			rightOp.reset(partitionCtr+partitionTraverser);
			b=rightOp.getNextTuple();

			partitionCtr= partitionCtr+partitionTraverser;


			System.out.println("B and A are no longer equal. The next B has this value set: " + b.getValues());
			System.out.println("the A has the following value set: " + a.getValues());

			while(b==null){

				b=rightOp.getNextTuple();

				partitionCtr++;
			}
			if(b.getTable().equals("ENDOFFILE") && !a.getTable().equals("ENDOFFILE") && !(joinExp instanceof EqualsTo))
			{

				rightOp.reset(beforeLoop); 
				b=rightOp.getNextTuple();




				partitionCtr=beforeLoop;
			}
		}

	}
	/**
	 * @
	 */
	public Tuple getNextTuple()
	{ 
		if(bufferSetup == false)
		{
			System.out.println("SMJ: buffer set up initiated");
			bufferSetup = true;
			bufferSetUp();

		}
		int cur = currentTuple;

		currentTuple=currentTuple+1;
		if(cur > tupleBuffer.keySet().size()-1)
		{ 
			System.out.println("my buffer size is: " + tupleBuffer.keySet().size());

			System.out.println("some how tupleBuffer size is not greater than or equal to 1..");
			System.out.println("I am at the the point before i get the tuples, my buffer size is: "+ bufferCtr);
			System.out.println("partition counter value is " + partitionCtr);
			Tuple t = new Tuple();
			t.setName("ENDOFFILE");
			return t;
		}
		else{
			System.out.println("getting stuff from this SMJOperator getNextTuple");
			return tupleBuffer.get(cur);
		}
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	@Override
	public void dump() throws IOException {
		// TODO Auto-generated method stub

	}
	public void setLeftOperator(SortOperator k)
	{
		leftOp= k;
	}
	/**
	 * @param k
	 * Sets the right Operator to k
	 */
	public void setRightOperator(SortOperator k)
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
	public Table getTableName()
	{
		return leftOp.getTable();
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
			if(accessor.contains("ESORT")){
				continue;
			}
			else{
				val1.put(accessor,(Integer)tuple1.getValues().get(accessor));
			}

		}

		Hashtable<String, Integer> val2= tuple2.getValues();
		for(int i =0; i < val2.size(); i++)
		{
			String accessor = (String)(val2.keySet().toArray()[i]);
			if(accessor.contains("ESORT")){
				continue;
			}
			else{
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
		}
		return val1;
	}

	@Override
	public void reset(int index) {
		// TODO Auto-generated method stub

	}
}