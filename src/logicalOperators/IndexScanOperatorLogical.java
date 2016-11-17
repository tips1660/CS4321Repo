package logicalOperators;

import java.io.File;
import java.io.IOException;

import net.sf.jsqlparser.schema.Table;

public class IndexScanOperatorLogical extends LogicalOperator {

	public Table getTableN() {
		return tableN;
	}
	public void setTableN(Table tableN) {
		this.tableN = tableN;
	}
	
	public File getFile() {
		return indexFile;
	}
	
	public void setFile(File f) {
		indexFile = f;
	}
	
	public String getAttr() {
		return attr;
	}
	
	public void setAttr(String s) {
		attr = s;
	}
	
	public int getCluster() {
		return cluster;
	}
	
	public void setCluster(int i) {
		cluster = i;
	}
	
	public int getHigh() {
		return high;
	}
	
	public void setHigh(int i) {
		high = i;
	}
	
	public int getLow() {
		return low;
	}
	
	public void setLow(int i) {
		low = i;
	}
	
	Table tableN;
	File indexFile;
	String attr;
	int cluster;
	int low;
	int high;
	@Override
	public void accept(PhysicalPlanBuilder s) throws IOException {
		// TODO Auto-generated method stub
	    s.visit(this);
		
	}

}
