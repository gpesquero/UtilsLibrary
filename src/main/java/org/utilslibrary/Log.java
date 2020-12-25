package org.utilslibrary;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;

public class Log {
	
	private static PrintWriter mLogFileWriter = null;
	
	private static String mLogDateString = null;
	
	private static boolean mShowDebugLogs = false;
	
	private static boolean mLogToFile = false;
	
	private static String mLogFileBaseName = "LogFile";
	
	private static String mLogDirName = "./logs";
	
	private static void log(String typeString, String text) {
		
		LocalDateTime time = LocalDateTime.now();
		
		String dateString=String.format("%04d-%02d-%02d",
				time.getYear(),
				time.getMonthValue(),
				time.getDayOfMonth());
		
		String timeString=String.format("%02d:%02d:%02d",
				time.getHour(),
				time.getMinute(),
				time.getSecond());
		
		String logLine = dateString + " " + timeString + " " + typeString + " " + text;
		
		// Log to console...
		System.out.println(logLine);
		
		if (!mLogToFile) {
			
			// Log to file is disabled....
			return;
		}
		
		if (mLogDateString != null) {
			
			// Check if dateString has changed...
			
			if (mLogDateString.compareTo(dateString) != 0) {
				
				// DateString has changed
				// Close existing log file to force the open of a new one...
				
				if (mLogFileWriter != null) {
					
					mLogFileWriter.close();
				
					mLogFileWriter = null;
				}
			}
		}
		
		if (mLogFileWriter == null) {
			
			// Create or open log file...
			
			FileWriter logFile;
			
			try {
				
				String logFileName = mLogFileBaseName + "_" + dateString + ".log";
				
				File logDir = new File(mLogDirName);
				
				if (!logDir.exists()) {
					
					if (!logDir.mkdirs()) {
						
						System.out.println(dateString + " " + timeString + " " + "((((ERROR)))) " +
								"Error in logDir.mkdirs()");
					}
				}
				
				// Create FileWriter. 'true' argument means append to existing file
				
				logFile = new FileWriter(mLogDirName + "/" + logFileName, true);
				
			} catch (IOException e) {
				
				System.out.println(dateString + " " + timeString + " " + "((((ERROR)))) " +
						e.getMessage());
				
				return;
			}
			
			mLogDateString = dateString;
			
			BufferedWriter bw = new BufferedWriter(logFile);
				
			mLogFileWriter = new PrintWriter(bw);
		}
		
		if (mLogFileWriter != null) {
			
			mLogFileWriter.println(logLine);
			
			mLogFileWriter.flush();
		}
	}
	
	public static void info(String text) {
		
		log("(INFO)", text);
	}

	public static void debug(String text) {
		
		if (mShowDebugLogs) {
			
			log("(DEBUG)", text);
		}
	}

	public static void warning(String text) {
		
		log("((((WARNING))))", text);		
	}

	public static void error(String text) {
		
		log("((((ERROR))))", text);		
	}
	
	public static void data(String text) {
		
		log("(DATA)", text);		
	}
	
	public static void showDebugLogs(boolean show) {
		
		mShowDebugLogs = show;
	}
	
	public static boolean isShowDebugEnabled() {
		
		return mShowDebugLogs;
	}
	
	public static void logToFile(boolean logToFile, String logFileBaseName, String logDirName) {
		
		mLogToFile = logToFile;
		
		mLogFileBaseName = logFileBaseName;
		
		mLogDirName = logDirName;
	}
	
	public static void closeLogFile() {
		
		if (mLogFileWriter != null) {
			
			mLogFileWriter.flush();
			
			mLogFileWriter.close();
			
			mLogFileWriter = null;
		}
	}
}
