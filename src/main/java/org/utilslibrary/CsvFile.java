package org.utilslibrary;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class CsvFile {
	
	private ArrayList<String> mColumnNames = null;
	
	private BufferedReader mReader = null;
	
	private int mLineCount = 0;
	
	public CsvFile() {
		
	}
	
	public boolean openFile(String fileName) {
		
		try {
			mReader = new BufferedReader(new FileReader(fileName));
			
		} catch (FileNotFoundException e) {
			
			Log.error("CSV openFile() error: " + e.getMessage());
			
			return false;
		}
		
		return true;
	}
	
	public ArrayList<String> getColumNames() {
		
		if (mColumnNames == null) {
			
			if (mReader != null) {
				
				try {
					String line = mReader.readLine();
					
					mColumnNames = getLineFields(line);
					
				} catch (IOException e) {
					
					Log.error("getColumnNames() readLine() error: " + e.getMessage());
				}		
			}
			else {
				
				Log.error("getColumnNames(): Reader is null");
			}
		}
		
		return mColumnNames;
	}
	
	public ArrayList<String> getData() {
		
		if (mColumnNames == null) {
			
			mColumnNames = getColumNames();		
		}
		
		ArrayList<String> fields = null;
		
		try {
			
			String line = mReader.readLine();
			
			if (line != null) {
				
				mLineCount++;
				
				fields = getLineFields(line);
				
				if (fields.size() != mColumnNames.size()) {
						
					Log.error("Column count does not match in line #" + mLineCount);
						
					fields = null;
				}
			}
			
		} catch (IOException e) {
			
			Log.error("Error in readLine(): " + e.getMessage());
			
			fields = null;
		}
		
		return fields;
	}
	
	private ArrayList<String> getLineFields(String line) {
		
		ArrayList<String> fields = new ArrayList<String>();
		
		int pos = 0;
		int commaPos;
		
		do {
			
			commaPos = line.indexOf(',', pos);
			
			String field;
			
			if (commaPos < 0) {
				
				field = line.substring(pos);
			}
			else {
				
				field = line.substring(pos, commaPos);
			}
			
			fields.add(field);
			
			pos = commaPos+1;
		}
		while(commaPos>=0);
		
		return fields;
	}
	
	public int getLineCount() {
		
		return mLineCount;
	}
}
