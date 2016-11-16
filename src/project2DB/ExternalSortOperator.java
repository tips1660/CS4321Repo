package project2DB;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import static java.nio.file.StandardCopyOption.*;

/*
 * TO DO LIST
 * 1. Rewrite TupleWriter to use a string input for entirety
 * 2. Get tuples in nextPass()
 * 3. Fix Parsertest
 * 4. Fix sortoperatorlogical and physicalplanbuilder if needed
 * 5. Figure out copy shit 
 * 6.
 */
/**
 * SortOperator is the class representing the sort operator.
 * 
 * @author Robert Cao rrc8 Pulkit Kashyap pk374
 */
public class ExternalSortOperator extends SortOperatorParent {

	ArrayList<Tuple> buffer = new ArrayList<Tuple>();
	static List<OrderByElement> sortList;
	static ArrayList<String> sortListActual;
	int ctr=0;
	ArrayList<SelectItem> items;
	ArrayList<String>columnList;
    boolean bufferInit = false;
    boolean calledSort = false;
    
    String dir = "";
    int b;
    int bufferSize=0;
    int numAttributes;
    Table table;
    int numTuples;
    int numPass;
    int numRuns = 0;
    HashMap<Integer, ArrayList<String>> streams = new HashMap<Integer, ArrayList<String>>();
    //TupleWriter writer;
    TupleReader reader;
    String out;
    boolean lastPass = false;
    ArrayList<TupleReader> readers = new ArrayList<TupleReader>();
    String fname = "";
    
	public ExternalSortOperator(String out, String tempDir, int bufferSize, int dirNum, ArrayList<SelectItem> items1, List<OrderByElement> orderList) throws IOException
	{
		this.out = out;
		dir = tempDir + File.separator + dirNum;
		File f = new File(dir);
		f.mkdir();
		b = bufferSize;
		items = items1;
		sortList = orderList;
		if(sortList==null)
			sortList = new ArrayList<OrderByElement>();
		sortListActual = new ArrayList<String>();
		
		System.out.println("ay");
		//columnList = ((ProjectOperator)this.getChild()).getColumnList();
		System.out.println("i knew it");
		/*if (items.get(0) instanceof AllColumns) {
			//get all columns from all lists, c
			int c = 10;
			numAttributes = 4088/c;
		}
		else {
			numAttributes = 4088/items.size();
		}*/
		numPass = 0;
		this.bufferSize = bufferSize;

		//ArrayList<String> firstPass = new ArrayList<String>();
		/*f = new File(dir + File.separator + numPass);
		f.mkdir();
		for (int i = 0; i < b-1; i++) {
			String outputdir = dir + File.separator + numPass + File.separator + i;
			firstPass.add(outputdir);
		}
		streams.put(numPass, firstPass);*/
		//numAttributes = columnList.size();
		//numTuples = 1022/numAttributes;
	}
	public ExternalSortOperator( String tempDir, int bufferSize, int dirNum, ArrayList<SelectItem> items1, List<OrderByElement> orderList) throws IOException
	{
		//this.out = out;
		dir = tempDir + File.separator + dirNum;
		File f = new File(dir);
		f.mkdir();
		this.bufferSize = bufferSize;
		b = bufferSize;
		items = items1;
		sortList = orderList;
		if(sortList==null)
			sortList = new ArrayList<OrderByElement>();
		sortListActual = new ArrayList<String>();
		
		System.out.println("ay");
		//columnList = ((ProjectOperator)this.getChild()).getColumnList();
		System.out.println("i knew it");
		/*if (items.get(0) instanceof AllColumns) {
			//get all columns from all lists, c
			int c = 10;
			numAttributes = 4088/c;
		}
		else {
			numAttributes = 4088/items.size();
		}*/
		numPass = 0;
		//ArrayList<String> firstPass = new ArrayList<String>();
		/*f = new File(dir + File.separator + numPass);
		f.mkdir();
		for (int i = 0; i < b-1; i++) {
			String outputdir = dir + File.separator + numPass + File.separator + i;
			firstPass.add(outputdir);
		}
		streams.put(numPass, firstPass);*/
		//numAttributes = columnList.size();
		//numTuples = 1022/numAttributes;
	}
	
	public void sort() throws IOException 
	{
		if (calledSort)
			return;
		calledSort = true;
		System.out.println(numPass);
		columnList = ((ProjectOperator)this.getChild()).getColumnList();
		for(int i =0; i< sortList.size(); i++)
		{
			sortListActual.add(sortList.get(i).toString());
		}
		for(int i = 0; i<columnList.size(); i++)
		{
			if (sortListActual.contains(columnList.get(i)))
				continue;
			else
				sortListActual.add(columnList.get(i));		
       }
		System.out.println(items.size());
		if (items.get(0) instanceof AllColumns)
			numAttributes = columnList.size();
		else
			numAttributes = items.size();
		System.out.println(1022/numAttributes);
		numTuples = 1022/numAttributes;
		pass0();
		//System.exit(0);
		while (!lastPass) 
		{
			System.out.println("Starting pass "+numPass);
			nextPass();
		}
		fname = dir + File.separator + "pass"+numPass + File.separator + 0;
	}
	public void setTable(Table tableN) {
		System.out.println("setting table");
		table = tableN;
		
	}
	public void pass0() throws IOException 
	{
		System.out.println("Starting pass 0");
		Tuple currentTuple;
		ArrayList<String> firstPass = new ArrayList<String>();
		File f = new File(dir + File.separator + "pass"+numPass);
		f.mkdir();
		boolean eof = false;
		System.out.println("got to this loop");
		ArrayList<Tuple> buffer0 = new ArrayList<Tuple>();

		while(true) 
		{
			numRuns++;
			System.out.println("about to change writer file");
				String outputdir = dir + File.separator + "pass"+numPass + File.separator + (numRuns-1+"");
				firstPass.add(outputdir);
				TupleWriter writer = new TupleWriter(firstPass.get(numRuns-1));
			System.out.println("changed writer file");
			b= bufferSize;

			System.out.println("b isCODEV4: " + b);
			for (int i = 0; i < b; i++) {
				System.out.println("got into external sort for loopCODEV1");
				if (eof)
					break;
				for (int j = 0; j < numTuples; j++) {
					System.out.println("got into external sort for loop 2CODEV2");
					currentTuple = child.getNextTuple();
					while (currentTuple == null)
						currentTuple = child.getNextTuple();
					//System.out.println("Values for this tuple gotten in pass 0 are: "+currentTuple.getValues() +" writer number is " + numWriter +" place in buffer is "+j);

					//System.out.println(currentTuple.getTable());
					if (currentTuple.getTable().equals("ENDOFFILE")) {
						buffer0.add(currentTuple);
						System.out.println("endoffile reached");
						eof = true;
						break;
					}
					setItemList(currentTuple);
					buffer0.add(currentTuple);
				}
			}
			try {
				System.out.println("am i just goign to this sort? CODEV3" );
				System.out.println("Buffer0 size is: " + buffer0.size() + " codev7");
				Collections.sort(buffer0, TupleComparator);
			}
			catch(Exception e) {
				System.out.println("caught an exception sorting this");
			}
			boolean end = false;
			for (int i = 0; i < buffer0.size(); i++) {
				currentTuple = buffer0.get(i);
				if (i == 0) {
					System.out.println("Writing very first tuple in pass 0 is "+currentTuple.getValues());
				}
				if (currentTuple.getTable().equals("ENDOFFILE")) {
					System.out.println("ENDOFFILE reached");
					end = true;
					try {
						writer.write();
					}
					catch (IOException e) {
						e.printStackTrace();
					}
					numPass++;
					break;
				}
				try {
					writer.writeTuple(currentTuple);
				}
				catch (IOException e) {
					System.out.println("writer couldn't write this tuple");
					e.printStackTrace();
				}
			}
			System.out.println("about to write");
			try {
				if (!end)
					writer.write();
			}
			catch (IOException e) {
				System.out.println("writer could not write");
				e.printStackTrace();
			}

			buffer0.clear();
			if (eof)
				break;
			}

		streams.put(0, firstPass);
		if (firstPass.size() == 1) {
			lastPass = true;
			numPass = 0;
		}
	}

	public void nextPass() throws IOException
	{
		//keep track of number of pages in previous pass
		System.out.println("Number of runs of pass: "+ (numPass-1) + " is " + numRuns);
		int prevRuns = numRuns;
		numRuns = (numRuns-1)/(b-1) + 1;
		System.out.println("Number of runs of pass: "+ (numPass) + " is " + numRuns);
		Tuple currentTuple;
		
		//boolean isDone = false;
		//ArrayList<String> pass = new ArrayList<String>();
		File f = new File(dir + File.separator + "pass"+numPass);
		f.mkdir();
		if (numRuns-1 == 0) {
			lastPass = true;
			System.out.println("Pass "+numPass +" is the final pass.");
			//numRuns = 1;

		}


		//streams.put(numPass, pass);
		//System.out.println("about to get previous pass list");
		
		//ArrayList<String> prevPass = streams.get(numPass-1);
		//System.out.println("prevpass size is " + prevPass.size());
		//writers.clear();


		int p = b* (int) Math.pow(b-1, numPass);
		System.out.println("the number of pages in pass "+numPass+" is " +p);
		
		
		PriorityQueue<TupleS> q = new PriorityQueue<TupleS>(TupleSComparator);
		

		int counter = 0;
		for (int x = 0; x < numRuns; x++) {
			String fname = dir + File.separator + "pass"+numPass + File.separator + x;
			TupleWriter writer = new TupleWriter(fname);
			//System.out.println("Getting new writer from " + x);
			readers.clear();
			for (int i = 0; i < b-1; i++) {
				if (counter < prevRuns) {
				String readerName = dir + File.separator + "pass"+(numPass-1) + File.separator + counter;
				TupleReader r = new TupleReader(readerName);
				readers.add(r);
				counter++;
				}
			}
			for (int i = 0; i < readers.size(); i++) {
				currentTuple = new Tuple(readers.get(i));
				setItemList(currentTuple);
				currentTuple.changeColumns();
				q.add(new TupleS(i, currentTuple));
				System.out.println(currentTuple.getTable());
				System.out.println(currentTuple.getValues().values());
			}
			while (!q.isEmpty()) {
					TupleS currentSTuple = q.poll();
					currentTuple = currentSTuple.getTuple();
					int readerNum = currentSTuple.getReader();
					//System.out.println("The value of the smallest tuple of q from file "+readerNum+" is " + currentTuple.getValues().values());
					//buffer.add(currentTuple);
					Tuple t2 = new Tuple(readers.get(readerNum));
					if (!t2.getTable().equals("ENDOFFILE")) {
						setItemList(t2);
						t2.changeColumns();
						//System.out.println("Adding a tuple with these values to q " + t2.getValues().values());
						TupleS ts2 = new TupleS(readerNum, t2);
						q.add(ts2);
					}
					else {
						//System.out.println("reader hit an endoffile");
					}


					writer.writeTuple(currentTuple);
				//writer.write();
				//buffer.clear();

			}
			writer.write();
		}
		numPass++;
		if (lastPass)
			numPass--;
	}


	public void setItemList(Tuple t) {
		ArrayList<String> itemList = new ArrayList<String>();
		
		if(!(items.get(0) instanceof AllColumns)){
			for(int j =0; j < items.size()-1; j++)
			{   
				String colName = ((Column)((SelectExpressionItem) items.get(j)).getExpression()).toString();
				itemList.add(colName);
			}
			String colName = ((Column)((SelectExpressionItem) items.get(items.size()-1)).getExpression()).toString();
			itemList.add(colName);
						
			//useful stuff
			t.setOutputOrder(itemList);
		}
		else{
			 t.setOutputOrder(columnList);
		}
	}

	/**
	 * Adds a new OrderByElement to the list containing them
	 * 
	 * @param op 
	 * 			OrderByElement to be added
	 */
	public void addToSortList(OrderByElement op)
	{
		if(sortList==null)
			sortList = new ArrayList<OrderByElement>();
		sortList.add(op);
	}
	
	/**
	 * gets the list of columns in order
	 * 
	 * @returns columnList
	 * 		the list of columns
	 */
	public ArrayList<String> getColumnList()
	{
		return columnList;
	}
	
	
	public static Comparator<Tuple> TupleComparator = new Comparator<Tuple>()
	{
		@Override
		/**
		 * Compares two tuples
		 * 
		 * @param o1, o2 
		 * Tuples
		 * @return value
		 * -1 if o1 < o2, 1 if o1 > o2, 0 if o1 = o2
		 */
		public int compare(Tuple o1, Tuple o2) {
			for(int i =0; i<sortListActual.size(); i++){
				//System.out.println("sorting on col: " + sortListActual.get(i));
			}
			for(int i =0; i< sortListActual.size(); i++)
			{  
				try {
				Integer a = (Integer) o1.getValues().get(sortListActual.get(i));
				Integer b = (Integer) o2.getValues().get(sortListActual.get(i));
		        
				if(a.intValue()<b.intValue())
					return -1;
				else if(a.intValue()>b.intValue())
					return 1;
				else
					continue;
				} catch(Exception e){
				//means this column isn't in my tuple
				continue;	
				}
			}
			return 0;
		}
	};
	
	public static Comparator<TupleS> TupleSComparator = new Comparator<TupleS>()
	{
		@Override
		/**
		 * Compares two tuples
		 * 
		 * @param o1, o2 
		 * Tuples
		 * @return value
		 * -1 if o1 < o2, 1 if o1 > o2, 0 if o1 = o2
		 */
		public int compare(TupleS o11, TupleS o22) {
			for(int i =0; i<sortListActual.size(); i++){
				//System.out.println("sorting on col: " + sortListActual.get(i));
			}
			for(int i =0; i< sortListActual.size(); i++)
			{  
				try {
					Tuple o1 = o11.getTuple();
					Tuple o2 = o22.getTuple();
				Integer a = (Integer) o1.getValues().get(sortListActual.get(i));
				Integer b = (Integer) o2.getValues().get(sortListActual.get(i));
		        
				if(a.intValue()<b.intValue())
					return -1;
				else if(a.intValue()>b.intValue())
					return 1;
				else
					continue;
				} catch(Exception e){
				//means this column isn't in my tuple
				continue;	
				}
			}
			return 0;
		}
	};
	
	/**
	 * getNextTuple() returns the next tuple of this sort operator, or an ENDOFFILE object if there are no more tuples
	 * 
	 * @returns returnedTuple
	 */
	@Override
	public Tuple getNextTuple() {
		if(!calledSort)
		{
			try {
				sort();
			} catch (IOException e) {
				System.out.println("Could not sort");
				e.printStackTrace();
			}
			try {
				reader = new TupleReader(fname);
			} catch (IOException e) {
				System.out.println("Could not create a reader");
				e.printStackTrace();
			}
		}
		Tuple currentTuple = new Tuple(reader);

		setItemList(currentTuple);

		try {
			currentTuple.changeColumns();

 		} catch (IOException e) {
			System.out.println("Could not change columns of this tuple");
			e.printStackTrace();
		}
		return currentTuple;
	
	}
	

   
	//@Override
	/**
	 * Dump prints out every tuple from the sort
	 */
	public void dump() {
		System.out.println("dumping");
		/*columnList = ((ProjectOperator)this.getChild()).getColumnList();
		for(int i =0; i< sortList.size(); i++)
		{
			sortListActual.add(sortList.get(i).toString());
		}
		for(int i = 0; i<columnList.size(); i++)
		{
			if (sortListActual.contains(columnList.get(i)))
				continue;
			else
				sortListActual.add(columnList.get(i));		
       }
		System.out.println(items.size());
		if (items.get(0) instanceof AllColumns)
			numAttributes = columnList.size();
		else
			numAttributes = items.size();
		System.out.println(1022/numAttributes);
		numTuples = 1022/numAttributes;*/
		try {
			System.out.println("starting sort");
			sort();
		} 
		catch (IOException e) {
			System.out.println("Could not dump");
			e.printStackTrace();
		}
		System.out.println("about to write file");
		System.out.println(numPass);
		System.out.println(fname);
		System.out.println(Paths.get(out).toString());
		try {
			Files.copy(Paths.get(fname), Paths.get(out), REPLACE_EXISTING);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("could not copy");
			e.printStackTrace();
		}
		System.out.println("wrote file");
		
	}

	@Override
	/**
	 * Resets this operator such that getNextTuple() will return the first tuple it originally would.
	 */
	public void reset() {
		
	}


	public void reset(int index) {
		try {
			reader = new TupleReader(fname);
		} 
		catch (IOException e) {
			System.out.println("Reset failed; could not create new reader");
			e.printStackTrace();
		}
		int counter = reader.getNumTuples();
		System.out.println("counter is: " + counter);
		System.out.println("index is: " + index);
		
		while (counter < index && index > counter+numTuples) {
			System.out.println("counter is: " + counter);
			System.out.println("index is: " + index);
			System.out.println("counter+numTuples is " + (counter+numTuples));
			System.out.println("reader num tuples is: " + reader.getNumTuples());
			try {
				reader.getNextPage();
			} 
			catch (IOException e) {
				System.out.println("Could not get next page.");
				e.printStackTrace();
			}
			counter += numTuples;
			System.out.println("reader num tuples is: " + reader.getNumTuples());

		}
		int difference = index - counter;
		if(difference < 0)
			difference = index;
		System.out.println("the difference value is: " + difference);
		for (int i = 0; i < difference; i++) {

			Tuple a =getNextTuple();
			System.out.println(a.getValues() + " this is my new ExtOpCode2");
		}
	}


	@Override
	public void addToSortListActual(String leftJoinExpR) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public Table getTable() {
		// TODO Auto-generated method stub
		System.out.println("getting table");
		return table;
	}
	
	

}
