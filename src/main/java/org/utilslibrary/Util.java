package org.utilslibrary;

import java.time.Duration;
import java.time.Instant;
import java.util.Locale;

public class Util {
	
	static public String timeFormat(Instant start, Instant end) {
		
		String text;
		
		long millis = Duration.between(start, end).toMillis();
		
		if (millis < 1000) {
			
			text = String.format("%d ms", millis);
		}
		else {
			
			if (millis < 10000) {
				
				text = String.format(Locale.ENGLISH, "%.1f s", (double)millis/1000.0);
			}
			else {
				
				long secs = millis/1000;
				
				if (secs < 60) {
					
					text = String.format(Locale.ENGLISH, "%d secs", secs);
				}
				else if (secs < 3600) {
					
					text = String.format(Locale.ENGLISH, "%dm %ds",
							secs/60, (secs % 60));
				}
				else {
					
					text = String.format(Locale.ENGLISH, "%dh %dm %ds",
						secs / 3600, (secs % 3600) / 60, (secs % 60));
				}
			}
		}
		
		return text;		
	}
}
