package project2DB;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import  java.util.*;
import logicalOperators.PhysicalPlanBuilder;

/**
 * This class makes a DatabaseCatalog using singleton method. 
 * @author Pulkit Kashyap pk374 Robert Cao rrc85
 */
public final class DatabaseCatalog {
	private static DatabaseCatalog dbcat = new DatabaseCatalog();
	private Hashtable<String, tableProperties> tableMatcher = new Hashtable<String, tableProperties>();
	static File schemaName;
	static File[] tableLocations;
	static File[] indexes;
	static String inputDirectory = "";
	HashMap<String, String> tableToIndex = new HashMap<String, String>();
	HashMap<String, Integer> indexToCluster = new HashMap<String, Integer>();


	private DatabaseCatalog() 
	{
	}
	
	/**
	 * Starts Schema Parsing
	 * @throws MalformedURLException
	 */
	public void startParseSchema() throws MalformedURLException
	{

		try {
			parseSchema();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 *  Sets the Schema to the passed in File
	 * 
	 * @param File
	 *            the node to be visited
	 */
	public void setSchemaName(File s)
	{
		schemaName = s;
	}
	
	/**
	 * Sets the file directory where all of the tables are stored
	 * 
	 * @param s
	 *            
	 */
	public void setTableLocation(File[] s)
	{
		tableLocations = s;
	}
	
	/**
	 * Method for obtaining the database catalog; just returns dbcat
	 * @return DatabaseCatalog
	 *            returns the DatabaseCatalog for lookup operations
	 */
	public static DatabaseCatalog getInstance()
	{
		return dbcat;
	}
	
	/**
	 * Method for obtaining the database catalog; just returns tableMatcher
	 * @return DatabaseCatalog
	 *            returns the DatabaseCatalog for lookup operations
	 */
	public Hashtable getTableCatalog()
	{
		return tableMatcher;
	}
	
	/**
	 * @param t
	 * Sets the Input Directory
	 */
	public void setInputDirectory(String t)
	{
		inputDirectory = t;
	}

	/**
	 * parseSchema takes in the absolute path to the schema file, and will scan
	 * each table for its schema, and then create the tableMatcher
	 * @throws FileNotFoundException, MalformedURLException
	 */
	public void parseSchema() throws FileNotFoundException, MalformedURLException
	{
		File schema = schemaName;//new File("C:\\Users\\pulki_000\\Desktop\\dbpracproj2\\samples\\input\\db\\schema.txt");
		Scanner scan = new Scanner(schema);
		while(scan.hasNextLine())
		{
			String currentLine = scan.nextLine();
			String tableName = "";
			tableProperties table = new tableProperties();
			int leftPtr =0;
			for(int i =0; i<currentLine.length(); i++)
			{  
				if(tableName == ""){
					if(currentLine.charAt(i) == ' ')	
					{
						tableName = currentLine.substring(leftPtr, i);
						leftPtr = i+1;
						table.setName(tableName);

					}

				}
				else{
					if(currentLine.charAt(i) == ' '){
						table.addColumn(currentLine.substring(leftPtr, i));
						leftPtr = i+1;
					}
				}
			} 
			table.addColumn(currentLine.substring(leftPtr, currentLine.length()));
			String s;
			for (int i = 0; i < tableLocations.length; i++) {
				String checker = inputDirectory + File.separator + "db" + File.separator + "data" + File.separator + tableName;
				//System.out.println(checker);
				//System.out.println(tableLocations[i].getPath());
				if (checker.equals(tableLocations[i].getPath())) {
					//System.out.println(tableLocations[i].toURI().toURL().toString().substring(5));
					table.setUrl(tableLocations[i].toURI().toURL().toString().substring(5));
				}

			}
			tableMatcher.put(tableName, table);
		}
		scan.close();
	}

	public void setIndexesDirectory(File[] idx) 
	{
		indexes = idx;
	}
	
	public String getIndex(String s)
	{
		return tableToIndex.get(s);
	}
	
	public int getCluster(String s) 
	{
		Integer i = indexToCluster.get(s);
		if (i == null)
			i = 0;
		return indexToCluster.get(s);
	}
	
	public File getFile(String s)
	{
		for (File f: indexes)
		{
			if (f.getName().equals(s))
				return f;
		}
		return null;
	}
	
	public void setTableToIndex(HashMap<String, String> m) {
		tableToIndex = m;
	}
	
	public void setIndexToCluster(HashMap<String, Integer> m) {
		indexToCluster = m;
	}
}
