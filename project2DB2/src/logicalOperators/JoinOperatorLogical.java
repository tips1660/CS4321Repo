package logicalOperators;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectItem;

public class JoinOperatorLogical extends LogicalOperator{

	Table tableN;
    List<Join> jList;
    Expression e;
    ArrayList<SelectItem> items;
	public Table getTableN() {
		return tableN;
	}
	public void setTableN(Table tableN) {
		this.tableN = tableN;
	}
	public List<Join> getjList() {
		return jList;
	}
	public void setjList(List<Join> jList) {
		this.jList = jList;
	}
	public Expression getE() {
		return e;
	}
	public void setE(Expression e) {
		this.e = e;
	}
	public void setItems(ArrayList<SelectItem> items1) {
		items = items1;
		// TODO Auto-generated method stub
		
	}
	public ArrayList<SelectItem> getItems()
	{
		return items;
	}
	
	@Override
	public void accept(PhysicalPlanBuilder s) throws IOException {
         s.visit(this);		
	}

}
