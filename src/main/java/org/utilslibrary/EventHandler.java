package org.utilslibrary;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;

public class EventHandler {
	
	private Semaphore mSemaphore = null;
	
	private ArrayList<Event> mEventList = null;
	
	public EventHandler() {
		
		mSemaphore = new Semaphore(0);
		
		mEventList = new ArrayList<Event>();		
	}
	
	synchronized public void addEvent(Event event) {
		
		mEventList.add(event);
		
		mSemaphore.release();
	}
	
	synchronized public Event getEvent() {
		
		Event event;
		
		if (mEventList.size()==0) {
			
			event = null;
		}
		else {
			
			event = mEventList.remove(0);
		}
		
		return event;
	}
	
	public Event waitForEvent() {
		
		Event event;
		
		try {
			mSemaphore.acquire();
			
			event = getEvent();
			
		} catch (InterruptedException e) {
			
			Log.error("EventHandler InterruptedException: " + e.getMessage());
			
			event = null;
		}
		
		return event;
	}
}