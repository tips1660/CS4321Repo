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

public class SortOperatorLogical extends LogicalOperator{
   public String getOut() {
		return out;
	}

	public void setOut(String out) {
		this.out = out;
	}
	
	public String getTemp() {
		return tempdir;
	}

	public void setTemp(String temp) {
		tempdir = temp;
	}

	public ArrayList<SelectItem> getItems1() {
		return items1;
	}

	public void setItems1(ArrayList<SelectItem> items1) {
		this.items1 = items1;
	}

	public List<OrderByElement> getOrderList() {
		return orderList;
	}

	public void setOrderList(List<OrderByElement> orderList) {
		this.orderList = orderList;
	}

	public int getQuery() {
		return querynum;
	}
	
	public void setQuery(int q) {
		querynum = q;
	}

	String out;
	String tempdir;
	ArrayList<SelectItem> items1;
	List<OrderByElement> orderList;
	int querynum;
	
	public void accept(PhysicalPlanBuilder s) throws IOException
	{
		s.visit(this);
	}
	
	
}
