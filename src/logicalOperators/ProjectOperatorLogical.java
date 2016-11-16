package logicalOperators;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectOperatorLogical extends LogicalOperator {
    public String getOut() {
		return out;
	}

	public void setOut(String out) {
		this.out = out;
	}

	public ArrayList<SelectItem> getItems() {
		return items;
	}

	public void setItems(ArrayList<SelectItem> items) {
		this.items = items;
	}

	public List<Join> getJoinList() {
		return joinList;
	}

	public void setJoinList(List<Join> joinList) {
		this.joinList = joinList;
	}

	public Table getTableN() {
		return tableN;
	}

	public void setTableN(Table tableN) {
		this.tableN = tableN;
	}

	public String out;
    public ArrayList<SelectItem> items;
    List<Join> joinList;
    Table tableN;
    
    public void accept(PhysicalPlanBuilder s) throws IOException
    {
    	s.visit(this);
    }
    
}
