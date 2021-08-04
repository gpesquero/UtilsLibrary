package org.utilslibrary;

import java.util.HashMap;

public class Event {
	
	private String mId = "UNKNOWN";
	
	HashMap<String, Object> mObjectMap = new HashMap<String, Object>();
	
	public Event() {
		
		mId = "UNKNOWN";
	}
	
	public Event(String id) {
		
		mId = id;
	}
	
	public void setId(String id) {
		
		mId = id;
	}
	
	public String getId() {
		
		return mId;
	}
	
	public void addObject(String key, Object object) {
		
		mObjectMap.put(key, object);
	}
	
	public Object getObject(String key) {
		
		return mObjectMap.get(key);
	}

}
