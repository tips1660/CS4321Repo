package logicalOperators;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import project2DB.*;

public class PhysicalPlanBuilder {
           
	Operator root;
	Operator current;
	
	int joinType;
	int joinBuffer;
	String tempDir;
	int sortType;
	int sortBuffer;
	
	int useIndexes;
	IndexVisitor visitor = new IndexVisitor();
	
	public PhysicalPlanBuilder(int j1, int j2, int s1, int s2, int u) {
		joinType = j1;
		joinBuffer = j2;
		sortType = s1;
		sortBuffer = s2;
		useIndexes = u;
	}
		
	public void setTempDir(String temp)
	{
		tempDir = temp;
	}
	public Operator getRoot()
	{
		return root;
	}
	public void reset()
	{
		root = null;
		current = null;
	}
	
	public void visit(ScanOperatorLogical s) throws IOException
	{
 		if(root == null)
		{
			root = new scanOperator(s.tableN);
			current = root;
			
		}
		else{
			scanOperator nextOp = new scanOperator(s.tableN);
			System.out.println(s.tableN.toString());
			current.setChild(nextOp);
			current = nextOp;
			
			System.out.println("i am here in my scan");
			if(s.child!=null) {
				System.out.println("accepting scan");
				s.child.accept(this);
			}
		}
	}
	public void visit(ProjectOperatorLogical s) throws IOException
	{
		System.out.println("I am at the project operator");
		if(root == null)
		{
			root = new ProjectOperator(s.out, s.items, s.joinList, s.tableN);
			current = root;
			if(s.child !=null)
			s.child.accept(this);

		}
		else
		{
				ProjectOperator nextOp = new ProjectOperator(s.items, s.joinList,  s.tableN);
			    current.setChild(nextOp);
			    current = nextOp;
			    if(s.child!=null)
			    s.child.accept(this);
		}
	}
	public void visit(DistinctOperatorLogical s) throws IOException{
		System.out.println("i got to distinct operator log");
		if(root == null)
		{
			root = new DuplicateEliminationOperator(s.out,s.items1,sortType,sortBuffer);
			current = root;
		    if(s.child!=null)
		     s.child.accept(this);

		}
		else
		{
		   DuplicateEliminationOperator nextOp = new DuplicateEliminationOperator(s.out,s.items1,sortType,sortBuffer);
		   current.setChild(nextOp);
		   current = nextOp;
		   if(s.child !=null)
		   s.child.accept(this);
		}
		
	}
	public void visit(SortOperatorLogical s) throws IOException
	{  
		if(root == null)
		{
			System.out.println("Creating sortoperator");
			if (sortType == 0)
				root = new SortOperator(s.out, s.items1,  s.orderList);
			else {
				root = new ExternalSortOperator(s.out, s.tempdir, sortBuffer, s.querynum, s.items1, s.orderList);
			}
			current = root;
			if(s.child!=null)
				s.child.accept(this);
		}
		else
		{
			Operator nextOp;
			if (sortType == 0)
				nextOp = new SortOperator(s.items1,  s.orderList);
			else
				nextOp = new ExternalSortOperator(s.out, s.tempdir, sortBuffer, s.querynum, s.items1, s.orderList);
			current.setChild(nextOp);
			current = nextOp;
			if(s.child!=null)
			{
				System.out.println(s.child.toString());
				s.child.accept(this);
			}
		}
	}
	public void visit(JoinOperatorLogical s) throws IOException {
 
		JoinOperatorSuper nextOp = new JoinOperatorSuper(tempDir, s.tableN, s.jList, s.e, joinType, joinBuffer, sortType, sortBuffer, useIndexes);
		current.setChild(nextOp);
		current = nextOp;
		if(s.child!=null){
			System.out.println("not null");
			s.child.accept(this);}
		System.out.println("Built my JoinOperatorSuper");
		
	}
	public void visit(SelectOperatorLogical s) throws IOException {
		// TODO Auto-generated method stub
		System.out.println(" I managed to make it to Select Operator Logic");
		if (useIndexes == 1) {
			System.out.println("Select Operator logical with indexes");
			String index = visitor.setIndex(s.getTableN().getWholeTableName());
			s.input.accept(visitor);
			visitor.buildExpressions();
			System.out.println("SelectOperator was successfully visited");
			boolean newChild = false;
			boolean fullIndex = false;
		
			IndexScanOperatorLogical childOp = null;
			Operator nextOp = null;
			if (!visitor.getIndexArray().isEmpty()) {
				System.out.println("there exists a helpful index in select");
				newChild = true; 
				
				int cluster = visitor.setCluster(index);
				File f = visitor.getFile();
				
				childOp = new IndexScanOperatorLogical();
				childOp.setHigh(visitor.getHigh());
				childOp.setLow(visitor.getLow());
				childOp.setTableN(s.getTableN());
				childOp.setCluster(cluster);
				childOp.setAttr(index);
				childOp.setFile(f);
				System.out.println("properly set indexscan child");
				if (visitor.getNotIndexExpression() != null) {
					s.setInput(visitor.getNotIndexExpression());
					System.out.println("the new select exp is "+s.getInput());
				}
				else {
					//there is no select condition, scan will handle all
					//nextOp will just be a indexscan with no child
					System.out.println("about to create indexscan as nextop");
					fullIndex = true;
					newChild = false;
					nextOp = new IndexScanOperator(s.getTableN(), f, index, cluster, visitor.getLow(), visitor.getHigh());
				}
			}
			System.out.println("made indexscanoperator");
			if (!fullIndex)
				nextOp = new SelectOperator(s.tableN, s.input);
			current.setChild(nextOp);
			current = nextOp;
			if (newChild) {
				s.setChild(childOp);
			}
		}
		else {
			SelectOperator nextOp = new SelectOperator(s.tableN, s.input);
			current.setChild(nextOp);
			current = nextOp;
		}
		
		if(s.getChild()!=null) {
			s.child.accept(this);
		}
	}

	public void visit(IndexScanOperatorLogical s) throws IOException {
		IndexScanOperator nextOp = new IndexScanOperator(s.getTableN(), s.getFile(), s.getAttr(), s.getCluster(), s.getLow(), s.getHigh());
		current.setChild(nextOp);
		current = nextOp;
			
		if(s.child!=null) {
			s.child.accept(this);
		}
	}
	
}
