package org.utilslibrary;

import org.openstreetmap.osmosis.core.domain.v0_6.Node;

public class MyNode {
	
	Node mNode = null;
	
	public MyNode(Node node) {
		
		mNode = node;
	}
	
	public Coord getCoord() {
		
		Coord coord = new Coord(mNode.getLatitude(), mNode.getLongitude());
		
		return coord;	
	}
}
