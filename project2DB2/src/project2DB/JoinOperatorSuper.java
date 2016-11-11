package project2DB;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.expression.BinaryExpression;

/**
 * JoinOperatorSuper is the structure that will build the join tree from the WHERE expression
 * 
 * @author Robert Cao rrc8 Pulkit Kashyap pk374
 */
public class JoinOperatorSuper extends Operator {
	private HashMap<String, Expression> soloMap = new HashMap<String, Expression>();
	private HashMap<String, Expression> joinMap  = new HashMap<String, Expression>(); 
	private ArrayList<Expression> rejectedJoins = new ArrayList<Expression>();
	private JoinTreeVisitor j;
	private List<Join> joinList;
	private Operator joinOp;
	int jCondition;
	private SMJOperator joinOpSMJ;
	Expression expression;
	Expression leftExp;
	Operator leftOp;
	int ctr=0;
	SortOperatorParent leftOpSort;
	SortOperatorParent joinTreeLeft;
	//ExternalSortOperator joinTreeLeftEx;
	//ExternalSortOperator leftOpExternal;
	Hashtable<String, String> tableAlias  = new Hashtable<String, String>();
	private DatabaseCatalog cat = DatabaseCatalog.getInstance();


	public JoinOperatorSuper(String tempDir, Table tableN, List<Join> jList, Expression e, int j1, int j2, int st, int sortBuffer) throws IOException
	{
		jCondition = j1;
		System.out.println("are we getting to join operator super appropriately");		 
		joinList = jList;	
		tableAlias= makeTableAlias(tableN, jList);
		expression =e;
		j = new JoinTreeVisitor(tableN, tableAlias);
		ArrayList<SelectItem> items1 = new ArrayList<SelectItem>();
		AllColumns a = new AllColumns();
		items1.add(a);
		if(e!=null){
			e.accept(j);
		}
		soloMap = j.getSoloMap();
		joinMap = j.getJoinMap();
		rejectedJoins = j.getRejectedJoins();
		leftExp = soloMap.get(tableN.getWholeTableName());
		Expression leftJoinExp = joinMap.get(((Table)joinList.get(0).getRightItem()).getWholeTableName());
		if (leftJoinExp == null) {

			if (!rejectedJoins.isEmpty()) {
				leftJoinExp = (BinaryExpression) rejectedJoins.get(0);

			}

		}
		String leftJoinExpR = ((BinaryExpression) leftJoinExp).getLeftExpression().toString();

		if (leftExp == null){
			leftOp = new scanOperator(tableN);
		}
		else{
			leftOp = new SelectOperator(tableN, leftExp);
		}

		tableProperties table = (tableProperties) cat.getTableCatalog().get(tableN.getWholeTableName());
		int numAttributes = table.getColumns().size();
		if (j1 == 0)
			joinOp = new JoinOperator(leftOp, joinList, soloMap, joinMap, rejectedJoins);
		else if (j1 == 1){//in future change it 
			joinOp = new BNLJOperator(leftOp, joinList, soloMap, joinMap, rejectedJoins, j2, numAttributes);
		}
		else {

			if (leftExp == null){
				scanOperator scanLeft = new scanOperator(tableN);
				ProjectOperator projectLeft = new ProjectOperator(items1, jList,tableN);
				projectLeft.setChild(scanLeft);
			 
					leftOpSort = new SortOperator(items1, null);
					leftOpSort.setTable(tableN);
					leftOpSort.addToSortListActual(leftJoinExpR);
					leftOpSort.setChild(projectLeft);
				 /*
				/*else // makes the external sort based smj
				{	
					System.out.println("should be here first for external w/ no select");
					leftOpExternal = new ExternalSortOperator(tempDir,sortBuffer,ctr,items1, null);
					ctr++;
					leftOpExternal.addToSortListActual(leftJoinExpR);
					leftOpExternal.setChild(projectLeft);
					System.out.println("should have made an external");

					// got rid of the table
				}*/
			}
			else{
				SelectOperator leftSelectOp = new SelectOperator(tableN, leftExp);
				ProjectOperator projectLeft  = new ProjectOperator( items1,jList, tableN);
				projectLeft.setChild(leftSelectOp);
 					leftOpSort = new SortOperator(items1, null);
					leftOpSort.addToSortListActual(leftJoinExpR);
					leftOpSort.setTable(tableN);
					leftOpSort.setChild(projectLeft);
				 
			/*	else
				{
					leftOpSort = new ExternalSortOperator(tempDir, sortBuffer, ctr,items1, null);
					ctr++;
					leftOpSort.addToSortListActual(leftJoinExpR);
					leftOpSort.setChild(projectLeft); 
				}*/

				// sort->project->select or scan
				//somewhere here need to add the logic to make a sort operator on the left kid
			}
			//if(st==0){
				System.out.println("correctly building this right?");
				joinOpSMJ = new SMJOperator(leftOpSort, joinList, soloMap, joinMap, rejectedJoins, j2, ctr, tempDir, false);
		//	}
			//else{
				//System.out.println("I have made the joinOpSMJ");
				//joinOpSMJ = new SMJOperator(leftOpExternal, joinList, soloMap, joinMap, rejectedJoins, sortBuffer, ctr, tempDir);
				//ctr++;
			//}
			

		}

		while (joinList.size() > 1) {
			Join first = joinList.remove(0);
			String s = ((Table) first.getRightItem()).getWholeTableName();
			tableProperties t = (tableProperties) cat.getTableCatalog().get(s);
			numAttributes += t.getColumns().size();
			if (j1 == 0)
				joinOp = new JoinOperator(joinOp, joinList, soloMap, joinMap, rejectedJoins);
			else if(j1==1) {
				joinOp = new BNLJOperator(joinOp, joinList, soloMap, joinMap, rejectedJoins, j2, numAttributes);
			}
			else
			{//for SMJ
				leftJoinExp = joinMap.get(((Table)joinList.get(0).getRightItem()).getWholeTableName());
				if (leftJoinExp == null) {

					if (!rejectedJoins.isEmpty()) {
						leftJoinExp = (BinaryExpression) rejectedJoins.get(0);

					}

				}
				ProjectOperator lastJoinProject = new ProjectOperator(items1,jList, joinOpSMJ.getTableName());
				lastJoinProject.setChild(joinOpSMJ);
				leftJoinExpR = ((EqualsTo) leftJoinExp).getLeftExpression().toString();
				//if(st==0){
					joinTreeLeft = new SortOperator(items1, null);
					joinTreeLeft.setTable(joinOpSMJ.getTableName());
					joinTreeLeft.addToSortListActual(leftJoinExpR);
					joinTreeLeft.setChild(lastJoinProject);
					((SortOperator) joinTreeLeft).setupBuffer();
					joinOpSMJ = new SMJOperator(joinTreeLeft, joinList, soloMap, joinMap, rejectedJoins, j2, ctr, tempDir, false);
					System.out.println("made that thing up");
				//}
				/*else
				{
					// join operator super needs to take in temp dir, it needs to take in buffer size, an
					joinTreeLeftEx = new ExternalSortOperator(tempDir, sortBuffer, ctr, items1, null);
					ctr++;
					//got rido f the table setting
					joinTreeLeftEx.addToSortListActual(leftJoinExpR);
					joinTreeLeftEx.setChild(lastJoinProject);
					joinTreeLeftEx.sort();
					joinOpSMJ = new SMJOperator(joinTreeLeftEx, joinList, soloMap, joinMap, rejectedJoins, j2, ctr, tempDir);
					ctr++;
				}*/
			}
		}
	}


	/**
	 * makeTableAlias constructs the alias' of every table, if they exist
	 * 
	 * @param tableN, jList
	 * @return tempHashTable
	 * the HashTable containing the alias of every table, if one exists
	 */
	public Hashtable<String, String> makeTableAlias(Table tableN, List<Join> jList)
	{
		Hashtable<String, String> tempHashTable = new Hashtable<String, String>();
		String alias = tableN.getAlias();
		String tableName = tableN.getWholeTableName();
		if (alias != null)
			tempHashTable.put(alias, tableName);
		else
			tempHashTable.put(tableName, tableName);

		for (int i = 0; i < joinList.size(); i++) {
			alias =((Table) joinList.get(i).getRightItem()).getAlias();
			tableName = ((Table) joinList.get(i).getRightItem()).getWholeTableName();
			if (alias != null)
				tempHashTable.put(alias, tableName);
			else
				tempHashTable.put(tableName, tableName);
		}
		return tempHashTable;


	}

	/**
	 * getNextTuple() returns the next tuple of the very top JoinOperator
	 * 
	 * @returns returnedTuple
	 */
	@Override
	public Tuple getNextTuple() {
		System.out.println("am i getting into this thing?");
		if(jCondition !=1 && jCondition!=0)
		return joinOpSMJ.getNextTuple();
		else
			return joinOp.getNextTuple();
 	}


	/**
	 * For testing purposes
	 */
	//@Override
	public void dump() {

	}


	/**
	 * Resets the Join Operator
	 */
	@Override
	public void reset() {
	}


	@Override
	public void reset(int index) {
		// TODO Auto-generated method stub
		
	}




}
