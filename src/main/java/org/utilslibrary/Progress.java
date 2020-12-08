package org.utilslibrary;

import java.util.Locale;

public class Progress {
	
	private int mNumberOfNodes=0;
	private int mNumberOfWays=0;
	private int mNumberOfRelations=0;
	
	private long mLastUpdateTime=0;
	
	private final static long UPDATE_TIME=5000;
	
	public synchronized void setNodeCount(int numberOfNodes) {
		
		mNumberOfNodes=numberOfNodes;
	}
	
	public synchronized void setWayCount(int numberOfWays) {
    	
		mNumberOfWays=numberOfWays;
    }
    
	public synchronized void setRelationCount(int numberOfRelations) {
    	
		mNumberOfRelations=numberOfRelations;
    }
	
	public synchronized void onNode(int nodeNumber) {
		
		if (checkTime()) {
			
			double percent=100.0*nodeNumber/mNumberOfNodes;
			
			String text=String.format(Locale.US, "Processing node %,d of %,d (%.01f%%)",
					nodeNumber, mNumberOfNodes, percent);
			
			System.out.println(text);
		}
	}
	
	public synchronized void onWay(int wayNumber) {
		
		if (checkTime()) {
			
			double percent=100.0*wayNumber/mNumberOfWays;
			
			String text=String.format(Locale.US, "Processing way %,d of %,d (%.01f%%)",
					wayNumber, mNumberOfWays, percent);
			
			System.out.println(text);
		}
	}
	
	public synchronized void onRelation(int relationNumber) {
	
		if (checkTime()) {
			
			double percent=100.0*relationNumber/mNumberOfRelations;
			
			String text=String.format(Locale.US, "Processing relation %,d of %,d (%.01f%%)",
					relationNumber, mNumberOfRelations, percent);
			
			System.out.println(text);
		}
	}

	private boolean checkTime() {
		
		boolean result;
		
		if (mLastUpdateTime==0) {
			
			mLastUpdateTime=System.currentTimeMillis();
			
			result=false;
		}
		else if ((System.currentTimeMillis()-mLastUpdateTime)<UPDATE_TIME) {
			
			return false;
		}
		else {
			
			mLastUpdateTime=System.currentTimeMillis();
			
			result=true;
		}
		
		return result;
	}
}
