package project2DB;

import java.util.Comparator;

public class RId {
	int pageId;
	int tupleId;

	public int getPageId() {
		return pageId;
	}
	public void setPageId(int pageId) {
		this.pageId = pageId;
	}
	public int getTupleId() {
		return tupleId;
	}
	public void setTupleId(int tupleId) {
		this.tupleId = tupleId;
	}
	public RId (int pageNum, int tupleNum)
	{
		this.pageId = pageNum;
		this.tupleId = tupleNum;
	}
	 public static Comparator<RId> ridComparator = new Comparator<RId>()
			 {
			@Override
			public int compare(RId o1, RId o2) {
				if(o1.getPageId() < o2.getPageId())
					return -1;
				else if(o1.getPageId() > o2.getPageId())
					return 1;
				else
					if ( o1.getTupleId() < o2.getTupleId() )
						return -1;
					else if(o1.getTupleId() > o2.getTupleId())
						return 1;
					else
						return 0;
				
			}
		};
	}
