package logicalOperators;

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
	
	public PhysicalPlanBuilder(int j1, int j2, int s1, int s2) {
		joinType = j1;
		joinBuffer = j2;
		sortType = s1;
		sortBuffer = s2;
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
 
		JoinOperatorSuper nextOp = new JoinOperatorSuper(tempDir, s.tableN, s.jList, s.e, joinType, joinBuffer, sortType, sortBuffer);
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
       SelectOperator nextOp = new SelectOperator(s.tableN, s.input);
       current.setChild(nextOp);
       current = nextOp;
      
       if(s.getChild()!=null){

    	   s.child.accept(this);
       }
	}
}
