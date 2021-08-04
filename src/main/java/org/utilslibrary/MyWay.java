package org.utilslibrary;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

public class MyWay  {
	
	private Way mWay = null;
	
	public enum Oneway {
		
		UNDEF,
		NO,
		FORWARD,
		BACKWARD
	}
	
	public MyWay(Way way) {
		
		mWay = way;
	}
	
	public MyWay(CommonEntityData data, List<WayNode> wayNodes) {
		
		mWay = new Way(data, wayNodes);
	}
	
	public Way getWay() {
		
		return mWay;
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

	public boolean hasJunctionRoundaboutTag() {
		
		boolean result = false;
		
		Collection<Tag> tags = mWay.getTags();
		
		Iterator<Tag> tagIter = tags.iterator();
		
		while(tagIter.hasNext()) {
			
			Tag tag = tagIter.next();
			
			if (tag.getKey().compareTo("junction") == 0) {
				
				if (tag.getValue().compareTo("roundabout") == 0) {
					
					result = true;
				}
				
				break;
			}
		}
		
		return result;
	}
	
	public boolean isRoundabout() {
		
		if (hasJunctionRoundaboutTag()==false) {
			
			// The way does not have the 'junction=roundabout' tag
			return false;
		}
		
		// This way is a roundabout if it's a closed way...	
		return mWay.isClosed();
	}
	
	public boolean containsNode(long nodeId) {
		
		List<WayNode> wayNodes = mWay.getWayNodes();
		
		Iterator<WayNode> iter = wayNodes.iterator();
		
		while(iter.hasNext()) {
			
			WayNode wayNode = iter.next();
			
			if (wayNode.getNodeId() == nodeId) {
				
				return true;
			}
		}
		
		return false;
	}
	
	public void updateWayNodeCoords(OsmDatabase database) {
		
		List<WayNode> wayNodes = mWay.getWayNodes();
		
		for(int i=0; i<wayNodes.size(); i++) {
			
			WayNode wayNode = wayNodes.get(i);
			
			Coord nodeCoord = database.getNodeCoord(wayNode.getNodeId());
			
			wayNodes.set(i, new WayNode(wayNode.getNodeId(), nodeCoord.mLat, nodeCoord.mLon));
		}
	}
}
