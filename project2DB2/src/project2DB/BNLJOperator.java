package project2DB;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;

@SuppressWarnings("unused")
public class BNLJOperator extends Operator {
	
	Operator leftOp;
	Operator rightOp;

	Expression rightExp;
	Expression joinExp;
	ExpressionTester testVisitor = new ExpressionTester();
	Tuple a;
	Tuple b;
	Tuple returnedTuple;
	int ctr=0;
	boolean endoffile = false;

	HashMap<String, Expression> soloMap = new HashMap<String, Expression>();
	HashMap<String, Expression> joinMap  = new HashMap<String, Expression>(); 
	ArrayList<Expression> rejectedJoins = new ArrayList<Expression>();
	List<Join> joinList;
	
	//buffer in BNLJ is an ArrayList
	private List<Tuple> outerBlock = null;
	private int tuplesPerPage = -1;
	private int numAttributes = -1;
	private int outerIndex = 0;
	private int numPages;
	private boolean finalBlock = false;
	private boolean staySame = false;
	private int counter = 0;
	Tuple currTuple = null;

	
	
	public BNLJOperator(Operator lOp, List<Join> jList, HashMap<String, Expression> solo, 
			HashMap<String, Expression> join, 
			ArrayList<Expression> rejected, int pages,int numA) throws FileNotFoundException {
		
		joinList = jList;
		leftOp = lOp;
		soloMap = solo;
		joinMap = join;
		rejectedJoins = rejected;

		rightExp = soloMap.get(((Table)joinList.get(0).getRightItem()).getWholeTableName());
		if (rightExp == null){
			rightOp = new scanOperator(((Table)joinList.get(0).getRightItem()));
		}
		else{
			rightOp = new SelectOperator(((Table)joinList.get(0).getRightItem()), rightExp);
		}

		joinExp = joinMap.get(((Table)joinList.get(0).getRightItem()).getWholeTableName());
		if (joinExp == null) {

			if (!rejectedJoins.isEmpty()) {
				joinExp = (BinaryExpression) rejectedJoins.get(0);

				rejectedJoins.remove(0);
			}

		}

		//block pre-processing
		numPages = pages;
		numAttributes = numA;
		tuplesPerPage = 1024 / numAttributes;
		outerBlock = new ArrayList<Tuple>();
		fillBuffer();
		//System.out.println("BNLJ constructor initialized");		
		
	}
	
	
	/**
	 *  Fill the buffer with the maximum amount of tuples allowed, according to numPages.
	 */
	
	private void fillBuffer(){
		//System.out.println("Calling fill buffer");
		
		if (finalBlock) return;
		
		outerBlock.clear();
		while(currTuple == null) currTuple = leftOp.getNextTuple();
		
		while (outerBlock.size() < tuplesPerPage * numPages){
			
			outerBlock.add(currTuple);
			//System.out.println("Adding tuple " +tuplecounter + " to buffer : "  + currTuple.getValues().values().toString());
			
			//We want to immediately stop filling the block if we reach EOF.
			if (currTuple.getTable().equals("ENDOFFILE")){
				System.out.println("ENDOFFILE reached");
				finalBlock = true;
				break;
			}
			
			currTuple = leftOp.getNextTuple();
			while(currTuple == null) currTuple = leftOp.getNextTuple();
		}
		//System.out.println("Total number of tuples in block: "+ outerBlock.size());
		setOuterIndex(0);	
	}
	
	
	private void setOuterIndex(int i){
		outerIndex = i;
		a = outerBlock.get(i);
		
	}

	@Override
	public Tuple getNextTuple() {
				
		//for each block B of R do 
		while (!a.getTable().equals("ENDOFFILE"))
		{
			//if staySame flag is true, then we do not want to increment B.
			if (!staySame)
			{
				b = null;
				while(b==null) b=rightOp.getNextTuple();
			}
			
			//for each tuple in S do 
			while (!(b.getTable().equals("ENDOFFILE")))
			{
				
				//for each tuple r in B do 
				while (outerIndex < outerBlock.size()){
					a = outerBlock.get(outerIndex);
					outerIndex++;
					
					//we are on the very last block
					if (a.getTable().equals("ENDOFFILE"))
					{
						finalBlock = true;
						//System.out.println("we are in the final block");
						break;
					}
					else
					{
						//some tuple pre-processing
						returnedTuple = new Tuple();
						returnedTuple.setName(a.getTable());
						Hashtable<String, Integer> vals = concatTuples(a,b);
						returnedTuple.setValues(vals);
						testVisitor.setTuple(returnedTuple);
						
						if(joinExp !=null)
						{
							joinExp.accept(testVisitor);
							if(testVisitor.getResult() == true)
							{
								counter++;
								staySame = true;
								//System.out.println(returnedTuple.getValues().values().toString());
								return returnedTuple;
							}
						}
						else 
						{	
							counter++;
							staySame = true;
							return returnedTuple;
						}
						
					}
				}
				staySame = false;
				
				//increment inner tuple
				outerIndex = 0;
				b = null;
				while(b==null) b=rightOp.getNextTuple();
		
			}
			
			if (!finalBlock)
			{
				staySame = false;
				//System.out.println("not final block, resetting right and refilling buffer");
				rightOp.reset();
				fillBuffer();
			}
		}
		
		System.out.println("Finished BNLJ");
		System.out.println("Total number of tuples returned:"+ counter);
		counter = 0;
		Tuple s = new Tuple();
		s.setName("ENDOFFILE");
		return s;
	}

	
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
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dump() throws IOException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void reset(int index) {
		// TODO Auto-generated method stub
		
	}



	
}
