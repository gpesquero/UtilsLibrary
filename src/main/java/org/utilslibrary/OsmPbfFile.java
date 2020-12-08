package org.utilslibrary;

import java.io.File;
import java.time.Instant;
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
	
	File mOsmFile=null;
	
	Progress mProgress=null;
    
    final private static int NUM_WORKERS = 3;
    
    public OsmPbfFile() {
		
	}
	
	public boolean openFile(String fileName) {
		
		System.out.print("Opening PBF file <"+fileName+">... ");
	
		mOsmFile = new File(fileName);
		
		if (!mOsmFile.canRead()) {
		
			System.out.println("ERROR: File cannot be read");
		
			return false;
		}
		
		System.out.println("Ok");
		
		return true;
	}
	
	public boolean getObjectCount() {
		
		System.out.println("getObjectCount() started...");
        
		Instant start=Instant.now();
		
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
        	
        	System.out.println("OsmosisRuntimeException: "+e.getMessage());          
        }
 
        System.out.println(" - "+numberOfNodes.get() + " nodes were found.");
        System.out.println(" - "+numberOfWays.get() + " ways were found.");
        System.out.println(" - "+numberOfRelations.get() + " relations were found.");
        
        Instant end=Instant.now();
        
        String text="getObjectCount() took "+Util.timeFormat(start, end);
        
        System.out.println(text);
        
        mProgress=new Progress();
        
        mProgress.setNodeCount(numberOfNodes.get());
        mProgress.setWayCount(numberOfWays.get());
        mProgress.setRelationCount(numberOfRelations.get());
        
        return true;
	}
	
	public boolean process(OsmDatabase db) {
		
		System.out.println("Processing OSM File:");
		
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
        	
        	System.out.println("OsmosisRuntimeException: "+e.getMessage());          
        }
 
        System.out.println(" - "+numberOfNodes.get() + " nodes were found.");
        System.out.println(" - "+numberOfWays.get() + " ways were found.");
        System.out.println(" - "+numberOfRelations.get() + " relations were found.");
        
        Instant end = Instant.now();
        
        System.out.println("Process time took "+Util.timeFormat(start, end));
        
        db.commit();
        
        db.setAutoCommit(true);
        
        return true;
	}
}
