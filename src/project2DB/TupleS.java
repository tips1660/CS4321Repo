package project2DB;

public class TupleS {

	public Tuple t;
	public int i;
	
	public TupleS(int i, Tuple t)
	{
		this.t = t;
		this.i = i;
	}
	
	public int getReader()
	{
		return i;
	}
	
	public void setReader(int i)
	{
		this.i = i;
	}
	
	public Tuple getTuple()
	{
		return t;
	}
	
	public void setTuple(Tuple t)
	{
		this.t = t;
	}
}
