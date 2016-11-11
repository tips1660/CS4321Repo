package logicalOperators;

import java.io.IOException;

import net.sf.jsqlparser.schema.Table;

public class ScanOperatorLogical extends LogicalOperator{

	 public Table getTableN() {
		return tableN;
	}
	public void setTableN(Table tableN) {
		this.tableN = tableN;
	}
	Table tableN;
	@Override
	public void accept(PhysicalPlanBuilder s) throws IOException {
		// TODO Auto-generated method stub
	    s.visit(this);
		
	}

}
