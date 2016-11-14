package project2DB;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import logicalOperators.*;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * Top level interpreter. Takes in the input and expect_output directories and performs the query.
 * 
 * @author Robert Cao rrc85 Pulkit Kashyap pk374
 */
public class parserTest {

	private static String queriesFile = "queries.sql";
	private static File pbconfigFile;
	static ExpressionTester  k = new ExpressionTester();


	public static void main(String[] args) throws IOException {
// in the readme write that this is hard coded, and that externalsortoperator works, but not with smj. Also we cannot delete files on some computers
		
		File config = new File(args[0]);
		FileReader cReader = new FileReader(config);
		BufferedReader configReader = new BufferedReader(cReader);
		
		File input = new File(configReader.readLine());
		File output = new File(configReader.readLine());
		File temp = new File(configReader.readLine());
		int buildIndex = Integer.parseInt(configReader.readLine());
		int evaluateQueries = Integer.parseInt(configReader.readLine());
	

		File[] inputDir = input.listFiles();
		File[] data = inputDir[1].listFiles();
		
		//Process plan_builder_config.txt
		pbconfigFile = inputDir[2];
		System.out.println(temp.getAbsolutePath());
		FileReader f = new FileReader(pbconfigFile);
		BufferedReader b = new BufferedReader(f);
		
		String sortLine = "";
		String joinLine ="";
		joinLine = b.readLine();
		sortLine = b.readLine();
		int sortType;
		int sortBuffer = 0;
		int joinType;
		int joinBuffer = 0;
		
		sortType = Integer.parseInt(sortLine.substring(0, 1));
		if (sortType == 1) sortBuffer = Integer.parseInt(sortLine.substring(2, sortLine.length()));
		
		joinType = Integer.parseInt(joinLine.substring(0, 1));
		if (joinType == 1) joinBuffer = Integer.parseInt(joinLine.substring(2, joinLine.length()));
		
		//Flag to specify whether PPB should use indexes for selection
		int useIndexes = Integer.parseInt(b.readLine());
		
		
		queriesFile = inputDir[3].getAbsolutePath();
		File[] tables = data[1].listFiles();
		File index_info = data[2];
		File[] indexes = data[3].listFiles();
		File schema = data[4];
		
		//System.out.println(schema.getAbsolutePath());

		DatabaseCatalog dbcat = DatabaseCatalog.getInstance();	
		dbcat.setSchemaName(schema);
		dbcat.setTableLocation(tables);
		dbcat.setInputDirectory(input.getPath());
		dbcat.setIndexesDirectory(indexes);
		dbcat.startParseSchema();
		
		//Feed each line of index_info.txt into new bulk loading B+ tree class
		BufferedReader indexReader = new BufferedReader(new FileReader(index_info));
		String index_line = "";
		
		while ((index_line = indexReader.readLine()) != null ){
			String[] split_line = index_line.split(" ");
			String table = split_line[0];
			String attribute = split_line[1];
			int clustered = Integer.parseInt(split_line[2]);
			int order = Integer.parseInt(split_line[3]);
			
			//Use these four values as arguments to construct your new B+ tree
			//Make sure that buildIndex outputs final trees into input/db/indexes/
			//Name the index files Relation+Attribute (i.e SailorsA)
			//buildIndex(table,attribute,clustered,order);
		
		}


		int counter = 1;

		LogicalOperator root;
		LogicalOperator current;
		PhysicalPlanBuilder buildPhys = new PhysicalPlanBuilder(joinType, joinBuffer, sortType, sortBuffer);
		buildPhys.setTempDir(temp+"");
		String out;



		try {
			CCJSqlParser parser = new CCJSqlParser(new FileReader(queriesFile));
			Statement statement;
			while ((statement = parser.Statement()) != null) {
				//out = new PrintStream(new FileOutputStream(output+"/query"+counter+".txt"));
				//System.setOut(out);
				//for(File f1 : temp.listFiles()){
	            //    deleteRecursive(f1);                
	            //}
				out = output + File.separator +"query" + counter+".txt";
				try{
					Select select = (Select) statement;
					PlainSelect t = (PlainSelect) select.getSelectBody();
					ArrayList<SelectItem> items = (ArrayList)t.getSelectItems();
					List<OrderByElement> s = t.getOrderByElements();
					Distinct distinctThing = t.getDistinct();
					// builds the logical tree if distinct query	 
					if(distinctThing!=null)
					{
						root = new DistinctOperatorLogical();
						((DistinctOperatorLogical) root).setOut(out);
						((DistinctOperatorLogical) root).setItems1(items);
						current = root;

						SortOperatorLogical second = new SortOperatorLogical();
						second.setOut(out);
						second.setItems1(items);
						second.setOrderList(s);
						second.setTemp(temp.getAbsolutePath());
						second.setQuery(counter);
						current.setChild(second);
						current = second;

						ProjectOperatorLogical third = new ProjectOperatorLogical();
						third.setItems(items);
						third.setJoinList(t.getJoins());
						third.setOut(out);
						third.setTableN((Table) t.getFromItem());
						current.setChild(third);
						current = third;

						// now gotta figure out if the next thing i make is a join, select, or scan                   
						if(t.getJoins() !=null)
						{
							JoinOperatorLogical fourth = new JoinOperatorLogical();
							fourth.setE(t.getWhere());
							fourth.setjList(t.getJoins());
							fourth.setTableN((Table) t.getFromItem());
							fourth.setChild(null);
							current.setChild(fourth);
							root.accept(buildPhys);
						}
						else if(t.getWhere()!=null)
						{
							SelectOperatorLogical fourth = new SelectOperatorLogical();
							System.out.println("building it here");
							fourth.setInput(t.getWhere());
							fourth.setTableN((Table)t.getFromItem());
							current.setChild(fourth);
							current = fourth;

							ScanOperatorLogical fifth = new ScanOperatorLogical();
							fifth.setTableN((Table)t.getFromItem());
							fifth.setChild(null);
							current.setChild(fifth);
							current = fifth;
							root.accept(buildPhys);
						}
						else
						{
							ScanOperatorLogical fourth = new ScanOperatorLogical();
							fourth.setTableN((Table) t.getFromItem());
							fourth.setChild(null);
							current.setChild(fourth); 
							root.accept(buildPhys);
						}

						long timeStart = System.currentTimeMillis();
						buildPhys.getRoot().dump(); 
						long timeEnd = System.currentTimeMillis();
						System.out.println("PKash$: The time to run query " + counter + " was: " + (timeEnd-timeStart));




						/*DuplicateEliminationOperator tester =
						new DuplicateEliminationOperator(out, items, t.getJoins(), t.getWhere(), (Table)t.getFromItem(), s);*/
					}
					else{

						if(s!=null)
						{
							System.out.println("starting");
							root = new SortOperatorLogical();
							((SortOperatorLogical) root).setOut(out);
							((SortOperatorLogical) root).setItems1(items);
							((SortOperatorLogical) root).setOrderList(s);
							((SortOperatorLogical) root).setTemp(temp.getAbsolutePath());
							((SortOperatorLogical) root).setQuery(counter);
							current = root;
							System.out.println("made sort");
							ProjectOperatorLogical third = new ProjectOperatorLogical();
							third.setItems(items);
							third.setJoinList(t.getJoins());
							third.setOut(out);
							third.setTableN((Table) t.getFromItem());
							current.setChild(third);
							current = third;
							System.out.println("made project");
						}
						else{

								System.out.println("got here again");
							root  = new ProjectOperatorLogical();
							((ProjectOperatorLogical) root).setItems(items);
							((ProjectOperatorLogical) root).setJoinList(t.getJoins());
							((ProjectOperatorLogical) root).setOut(out);
							((ProjectOperatorLogical) root).setTableN((Table) t.getFromItem());
							current = root;

						}
						System.out.println("hs");
						if(t.getJoins() !=null)
						{
							System.out.println("making join");
							JoinOperatorLogical fourth = new JoinOperatorLogical();
							fourth.setE(t.getWhere());
							fourth.setjList(t.getJoins());
							fourth.setTableN((Table) t.getFromItem());
							fourth.setChild(null);
							current.setChild(fourth);
							System.out.println("we were able to make a join operator logical");
							root.accept(buildPhys);
						}
						else if(t.getWhere()!=null)
						{
							SelectOperatorLogical fourth = new SelectOperatorLogical();
							fourth.setInput(t.getWhere());
							fourth.setTableN((Table)t.getFromItem());
							System.out.println(((Table) t.getFromItem()).toString());
							System.out.println(t.getWhere().toString());
							current.setChild(fourth);
							current = fourth; 
							

							ScanOperatorLogical fifth = new ScanOperatorLogical();
							fifth.setTableN((Table)t.getFromItem());
							fifth.setChild(null);
							current.setChild(fifth);
							if(current.getChild() instanceof ScanOperatorLogical)
								System.out.println("ok");
							if(fourth.getChild() instanceof ScanOperatorLogical)
								System.out.println("very ok");
							
							current = fifth;
							root.accept(buildPhys);
							
							System.out.println(" made the logical forms of scan and select");
						}
						else
						{   System.out.println("made scan");
							ScanOperatorLogical fourth = new ScanOperatorLogical();
							fourth.setTableN((Table) t.getFromItem());
							fourth.setChild(null);
							current.setChild(fourth); 
							root.accept(buildPhys);
						}


						long timeStart = System.currentTimeMillis();
						buildPhys.getRoot().dump(); 
						long timeEnd = System.currentTimeMillis();
						System.out.println("PKash$: The time to run query " + counter + " was: " + (timeEnd-timeStart));

					}
					counter++;
					root = null;
					buildPhys.reset();


				}catch(NullPointerException e)
				{
					//out = new PrintStream(new FileOutputStream(output+"/query"+counter+".txt"));
					//System.setOut(out);
					out = output+"/query"+counter+".txt";
					counter++;
					root = null;
					buildPhys.reset();
				}



			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
	
	static void deleteRecursive(File file) throws IOException{
        
        if( file.isDirectory() ){
            
            for(File f : file.listFiles()){
                deleteRecursive(f);                
            }
            
        }
        
        if(!file.delete()){
            throw new IOException("File cannot be deleted: " + file);
        }
 
    }


}