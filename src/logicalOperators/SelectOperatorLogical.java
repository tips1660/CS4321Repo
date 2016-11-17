package logicalOperators;

import java.io.IOException;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

public class SelectOperatorLogical extends LogicalOperator{
  Table tableN;
  public Table getTableN() {
	return tableN;
}
public void setTableN(Table tableN) {
	this.tableN = tableN;
}
public Expression getInput() {
	return input;
}
public void setInput(Expression input) {
	this.input = input;
}
Expression input;
	@Override
	public void accept(PhysicalPlanBuilder s) throws IOException {
		// TODO Auto-generated method stub
		s.visit(this);
	}

}
