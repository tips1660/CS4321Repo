package project2DB;

import java.util.ArrayList;

/**
 * TableProperties represents the file path of the table, the name of the table, as well as
 * the columns of the table.
 * 
 * @author Robert Cao rrc8 Pulkit Kashyap pk374
 */
public class tableProperties {

	private String url;
	private String name;
	private ArrayList<String> columns = new ArrayList <String>();


	/**
	 * Sets the file location of the table
	 * 
	 * @param fileloc
	 * the location of the file
	 */
	public void setUrl(String fileLoc)
	{
		url = fileLoc;
	}
	
	/**
	 * Sets the name of the table
	 * 
	 * @param table
	 * the name of the table
	 */
	public void setName(String table)
	{
		name  = table;
	}
	
	/**
	 * Adds a column to the table
	 * 
	 * @param col
	 * Column to be added
	 */
	public void addColumn(String col)
	{
		columns.add(col);
	}

	/**
	 * This method returns the columns of the table.
	 * 
	 * @return columns
	 * the columns of the table
	 */
	public ArrayList<String> getColumns()
	{
		return columns;
	}
	
	/**
	 * This method returns the name of the table.
	 * 
	 * @return name
	 * the name of the table
	 */
	public String getName()
	{
		return name;
	}
	
	/**
	 * This method returns the name of the table's file
	 * 
	 * @return url
	 * the location of the file
	 */
	public String getUrl()
	{
		return url;
	}
}