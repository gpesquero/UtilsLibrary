package org.utilslibrary;

import java.io.File;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.pbf2.v0_6.PbfReader;

public class OsmPbfFile {
	
	File mOsmFile = null;
	
	Progress mProgress = null;
    
    final private static int NUM_WORKERS = 3;
    
    public String mFileDateStamp = null;
    public String mFileTimeStamp = null;
    
    public double mMinLat = 100.0;
    public double mMaxLat = -100.0;
    public double mMinLon = 200.0;
    public double mMaxLon = -200.0;
    
    public OsmPbfFile() {
		
	}
	
	public boolean openFile(String fileName) {
		
		mOsmFile = new File(fileName);
		
		if (!mOsmFile.canRead()) {
		
			Log.error("Failed to open PBF file <" + fileName + ">. File cannot be read");
		
			return false;
		}
		
		Log.info("PBF file <" + fileName + "> opened OK !!");
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			
		mFileDateStamp = dateFormat.format(mOsmFile.lastModified());
		
		SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
		
		mFileTimeStamp = timeFormat.format(mOsmFile.lastModified());
		
		Log.info("PBF File modified date=" + mFileDateStamp + ", time="+mFileTimeStamp);
	
		return true;
	}
	
	public boolean getObjectCount() {
		
		Log.info("Getting PBF object count...");
        
		Instant start = Instant.now();
		
		AtomicInteger numberOfNodes = new AtomicInteger();
		AtomicInteger numberOfWays = new AtomicInteger();
		AtomicInteger numberOfRelations = new AtomicInteger();
        
        PbfReader reader = new PbfReader(mOsmFile, NUM_WORKERS);
		
		Sink sinkImplementation = new Sink() {
 
        	@Override
			public void process(EntityContainer entityContainer) {
 
                Entity entity = entityContainer.getEntity();
                
                if (entity instanceof Node) {
                	
                    numberOfNodes.incrementAndGet();
                    
                    Node node = (Node) entity;
                    
                    double lat = node.getLatitude();
                    double lon = node.getLongitude();
                    
                    if (lat < mMinLat)
                    	mMinLat = lat;
                    
                    if (lat > mMaxLat)
                    	mMaxLat = lat;
                    
                    if (lon < mMinLon)
                    	mMinLon = lon;
                    
                    if (lon > mMaxLon)
                    	mMaxLon = lon;
                    
                }
                else if (entity instanceof Way) {
                
                	numberOfWays.incrementAndGet();
                }
                else if (entity instanceof Relation) {
                	
                    numberOfRelations.incrementAndGet();
                }
            }
 
            @Override
			public void initialize(Map<String, Object> arg0) {
            }
 
            public void complete() {
            }
 
            @Override
			public void close() {
			}
        };
 
        reader.setSink(sinkImplementation);
        
        try {
        
        	reader.run();
        }
        catch(OsmosisRuntimeException e) {
        	
        	Log.error("OsmosisRuntimeException: " + e.getMessage());          
        }
 
        Log.info(String.format(Locale.US, " - %,d nodes were found", numberOfNodes.get()));
        Log.info(String.format(Locale.US, " - %,d ways were found", numberOfWays.get()));
        Log.info(String.format(Locale.US, " - %,d relations were found", numberOfRelations.get()));
        
        Log.info(String.format(Locale.US, " - MinLon= %3.5f", mMinLon));
        Log.info(String.format(Locale.US, " - MaxLon= %3.5f", mMaxLon));
        Log.info(String.format(Locale.US, " - MinLat= %3.5f", mMinLat));
        Log.info(String.format(Locale.US, " - MaxLat= %3.5f", mMaxLat));
        
        Instant end = Instant.now();
        
        String text = "PBF object count() took " + Util.timeFormat(start, end);
        
        Log.info(text);
        
        mProgress = new Progress();
        
        mProgress.setNodeCount(numberOfNodes.get());
        mProgress.setWayCount(numberOfWays.get());
        mProgress.setRelationCount(numberOfRelations.get());
        
        return true;
	}
	
	public boolean process(OsmDatabase db) {
		
		Log.info("Processing OSM File...");
		
		db.setAutoCommit(false);
		
		Instant start = Instant.now();
		
		PbfReader reader = new PbfReader(mOsmFile, NUM_WORKERS);
		
		AtomicInteger numberOfNodes = new AtomicInteger();
        AtomicInteger numberOfWays = new AtomicInteger();
        AtomicInteger numberOfRelations = new AtomicInteger();
        
        Sink sinkImplementation = new Sink() {
 
        	@Override
			public void process(EntityContainer entityContainer) {
 
                Entity entity = entityContainer.getEntity();
                
                if (entity instanceof Node) {
                	
                    numberOfNodes.incrementAndGet();
                    
                    Node node=(Node)entity;
                    
                    db.addNode(node);
                    
                    if (mProgress!=null) {
                    	
                    	mProgress.onNode(numberOfNodes.get());
                    }
                }
                else if (entity instanceof Way) {
                
                	numberOfWays.incrementAndGet();
                	
                	Way way=(Way)entity;
                	
                	db.addWay(way);
                	
                	if (mProgress!=null) {
                    	
                    	mProgress.onWay(numberOfWays.get());
                    }
                }
                else if (entity instanceof Relation) {
                	
                    numberOfRelations.incrementAndGet();
                    
                    Relation relation=(Relation)entity;
                    
                    db.addRelation(relation);
                    
                    if (mProgress!=null) {
                    	
                    	mProgress.onRelation(numberOfRelations.get());
                    }
                }
            }
 
            @Override
			public void initialize(Map<String, Object> arg0) {
            }
 
            public void complete() {
            }
 
            @Override
			public void close() {
			}
        };
 
        reader.setSink(sinkImplementation);
        
        try {
            
        	reader.run();
        }
        catch(OsmosisRuntimeException e) {
        	
        	Log.error("OsmosisRuntimeException: "+e.getMessage());          
        }
 
        Log.info(String.format(Locale.US, " - %,d nodes processed", numberOfNodes.get()));
        Log.info(String.format(Locale.US, " - %,d ways processed", numberOfWays.get()));
        Log.info(String.format(Locale.US, " - %,d relations processed", numberOfRelations.get()));
        
        Instant end = Instant.now();
        
        Log.info("Process time took " + Util.timeFormat(start, end));
        
        db.commit();
        
        db.setAutoCommit(true);
        
        return true;
	}
}
