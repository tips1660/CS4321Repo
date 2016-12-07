package logicalOperators;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;

public class DistinctOperatorLogical extends LogicalOperator {
   
	String out;
	public String getOut() {
		return out;
	}


	public void setOut(String out) {
		this.out = out;
	}


	public ArrayList<SelectItem> getItems1() {
		return items1;
	}


	public void setItems1(ArrayList<SelectItem> items1) {
		this.items1 = items1;
	}


	public List<Join> getJoinList() {
		return joinList;
	}


	public void setJoinList(List<Join> joinList) {
		this.joinList = joinList;
	}


	public Expression getSelect() {
		return select;
	}


	public void setSelect(Expression select) {
		this.select = select;
	}


	public Table getTableN() {
		return tableN;
	}


	public void setTableN(Table tableN) {
		this.tableN = tableN;
	}


	public List<OrderByElement> getOrderList() {
		return orderList;
	}


	public void setOrderList(List<OrderByElement> orderList) {
		this.orderList = orderList;
	}


	ArrayList<SelectItem> items1;
	List<Join> joinList;
	Expression select;
	Table tableN;
	List<OrderByElement> orderList;
	public void setChild(LogicalOperator t)
	{
		child = t;
	}
	public LogicalOperator getChild()
	{
		return child;
	}
	
	public void accept(PhysicalPlanBuilder s) throws IOException
	{
		s.visit(this);
	}

}
