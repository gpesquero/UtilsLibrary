package org.utilslibrary;

import java.util.Collection;
import java.util.Iterator;

import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;

public class MyWay  {
	
	Way mWay = null;
	
	public enum Oneway {
		
		UNDEF,
		NO,
		FORWARD,
		BACKWARD
	}
	
	/*
	public final int NO_ONEWAY=0;
	public final int ONEWAY_FORWARD=1;
	public final int ONEWAY_BACKWARD=2;
	*/
	
	public MyWay(Way way) {
		
		mWay=way;
	}
	
	public long getFirstNodeId() {
		
		return mWay.getWayNodes().get(0).getNodeId();
	}
	
	public long getLastNodeId() {
		
		return mWay.getWayNodes().get(mWay.getWayNodes().size()-1).getNodeId();
	}

	public Oneway getOneway() {
		
		Oneway oneway = Oneway.UNDEF;
		
		Collection<Tag> tags = mWay.getTags();
		
		Iterator<Tag> tagIter = tags.iterator();
		
		while(tagIter.hasNext()) {
			
			Tag tag = tagIter.next();
			
			if (tag.getKey().compareTo("oneway") == 0) {
				
				String value = tag.getValue();
				
				if (value.compareTo("yes") == 0) {
					
					oneway = Oneway.FORWARD;
				}
				else if (value.compareTo("no") == 0) {
					
					oneway = Oneway.NO;
				}
				else if (value.compareTo("-1") == 0) {
					
					oneway = Oneway.BACKWARD;
				}
				else {
					
					Log.warning("MyWay.getOneway(): Unknown <oneway> tag value '" + value + "'");
					
					oneway = Oneway.NO;
				}
				
				break;				
			}
		}
		
		return oneway;
	}

	public String getHighwayType() {
		
		String type=null;
		
		Collection<Tag> tags=mWay.getTags();
		
		Iterator<Tag> tagIter=tags.iterator();
		
		while(tagIter.hasNext()) {
			
			Tag tag=tagIter.next();
			
			if (tag.getKey().compareTo("highway")==0) {
				
				type=tag.getValue();
				
				break;				
			}
		}
		
		return type;
	}

	public boolean isRoundabout() {
		
		boolean result=false;
		
		Collection<Tag> tags=mWay.getTags();
		
		Iterator<Tag> tagIter=tags.iterator();
		
		while(tagIter.hasNext()) {
			
			Tag tag=tagIter.next();
			
			if (tag.getKey().compareTo("junction")==0) {
				
				if (tag.getValue().compareTo("roundabout")==0) {
					
					result=true;
				}
				
				break;				
			}
		}
		
		return result;
	}
}
