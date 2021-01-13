package org.utilslibrary;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.OsmUser;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.RelationMember;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

public class OsmDatabase {
	
	static final String DB_URL_PREFIX = "jdbc:sqlite:";
	
	static final String DB_VERSION = "1.00";
	
	Connection mConn = null;
	
	private String mSqlInsertNode = "INSERT INTO nodes(id, lon, lat) VALUES(?,?,?)";
	private String mSqlInsertNodeTags = "INSERT INTO node_tags(node_id, key, value) VALUES(?,?,?)";
	
	private String mSqlInsertWay = "INSERT INTO ways(id) VALUES(?)";
	private String mSqlInsertWayTags = "INSERT INTO way_tags(way_id, key, value) VALUES(?,?,?)";
	private String mSqlInsertWayNodes = "INSERT INTO way_nodes (way_id, sequence, node_id) VALUES(?,?,?)";
		
	private String mSqlInsertRelation = "INSERT INTO relations(id) VALUES(?)";
	private String mSqlInsertRelationTags = "INSERT INTO relation_tags(rel_id, key, value) VALUES(?,?,?)";
	private String mSqlInsertRelationMembers = "INSERT INTO relation_members (rel_id, sequence, member_type, member_id, member_role) VALUES(?,?,?,?,?)";
	
	private PreparedStatement mPrepStmtInsertNode = null;
	private PreparedStatement mPrepStmtInsertNodeTags = null;
	
	private PreparedStatement mPrepStmtInsertWay = null;
	private PreparedStatement mPrepStmtInsertWayTags = null;
	private PreparedStatement mPrepStmtInsertWayNodes = null;
	
	private PreparedStatement mPrepStmtInsertRelation = null;
	private PreparedStatement mPrepStmtInsertRelationTags = null;
	private PreparedStatement mPrepStmtInsertRelationMembers = null;
	
	public String mVersion;
	public String mCreationDate;
	public String mCreationTime;
	public double mMinLon, mMaxLon;
	public double mMinLat, mMaxLat;
	public String mPbfDateStamp;
	public String mPbfTimeStamp;
	
	public OsmDatabase() {
		
	}
	
	public boolean openDatabase(String fileName) {
		
		// Open a connection
		Log.info("Opening SQLite database <" + fileName + ">: ");
					
		try {
			mConn = DriverManager.getConnection(DB_URL_PREFIX + fileName);
			
		} catch (SQLException e) {
			
			Log.error("DriverManager getConnection error: " + e.getMessage());
			
			return false;
		}
		
		Log.info("Database <" + fileName + "> opened successfully");
		
		return true;
	}
	
	public void closeDatabase() {
		
		try {
			mConn.close();
			
		} catch (SQLException e) {
			
			Log.error("Error closing database: " + e.getMessage());
		}
		
		mConn = null;
	}
	
	public boolean createDatabase(String fileName) {
		
		Statement stmt = null;
		String sql = null;
		
		String logText = "";
		
		try {
			
			// Open a connection
			Log.info("Creating SQLite database <" + fileName + ">...");
			
			logText = " - Establishing SQLite Connection... ";
			mConn = DriverManager.getConnection(DB_URL_PREFIX+fileName);
			Log.info(logText + "Ok!!");
			
			stmt = mConn.createStatement();
			
			logText = " - Deleting table <db_info>... ";
			sql = "DROP TABLE IF EXISTS db_info";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Deleting table <nodes>... ";
			sql = "DROP TABLE IF EXISTS nodes";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Deleting table <node_tags>... ";
			sql = "DROP TABLE IF EXISTS node_tags";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Deleting table <ways>... ";
			sql = "DROP TABLE IF EXISTS ways";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Deleting table <way_tags>... ";
			sql = "DROP TABLE IF EXISTS way_tags";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Deleting table <way_nodes>... ";
			sql = "DROP TABLE IF EXISTS way_nodes";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Deleting table <relations>... ";
			sql = "DROP TABLE IF EXISTS relations";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Deleting table <relation_tags>... ";
			sql = "DROP TABLE IF EXISTS relation_tags";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Deleting table <relation_members>... ";
			sql = "DROP TABLE IF EXISTS relation_members";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Create table <db_info>... ";
			sql = "CREATE TABLE db_info (\n"
					+ "	version TEXT,\n"
	                + "	creation_date TEXT,\n"
	                + "	creation_time TEXT,\n"
	                + "	min_lon REAL,\n"
	                + "	max_lon REAL,\n"
	                + "	min_lat REAL,\n"
	                + "	max_lat REAL,\n"
	                + "	pbf_date TEXT,\n"
	                + "	pbf_time TEXT\n"
	                + ");";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Create table <nodes>... ";
			sql = "CREATE TABLE nodes (\n"
					+ "	id INTEGER PRIMARY KEY,\n"
	                + "	lon REAL,\n"
	                + "	lat REAL\n"
	                + ");";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Create table <node_tags>... ";
			sql = "CREATE TABLE node_tags (\n"
					+ "	node_id INTEGER,\n"
		    		+ " key TEXT,\n"
		    		+ " value TEXT\n"
	                + ");";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Create table <ways>... ";
			sql = "CREATE TABLE ways (\n"
					+ "	id INTEGER PRIMARY KEY\n"
	                + ");";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Create table <way_tags>... ";
			sql = "CREATE TABLE way_tags (\n"
					+ "	way_id INTEGER,\n"
		    		+ " key TEXT,\n"
		    		+ " value TEXT\n"
	                + ");";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Create table <way_nodes>... ";
			sql = "CREATE TABLE way_nodes (\n"
					+ "	way_id INTEGER,\n"
					+ " sequence INTEGER,\n"
					+ " node_id INTEGER\n"
		    		+ ");";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Create table <relations>... ";
			sql = "CREATE TABLE relations (\n"
					+ "	id INTEGER PRIMARY KEY\n"
	                + ");";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Create table <relation_tags>... ";
		    sql = "CREATE TABLE relation_tags (\n"
					+ "	rel_id INTEGER,\n"
		    		+ " key TEXT,\n"
		    		+ " value TEXT\n"
	                + ");";
		    stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Create table <relation_members>... ";
			sql = "CREATE TABLE relation_members (\n"
					+ "	rel_id INTEGER,\n"
					+ " sequence INTEGER,\n"
		    		+ " member_type INTEGER,\n"
		    		+ " member_id INTEGER,\n"
		    		+ " member_role TEXT\n"
	                + ");";
		    stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Creating index of table <node_tags>... ";
			sql = "CREATE INDEX node_tags_index ON node_tags(node_id)";
		    stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Creating index of table <way_tags>... ";
			sql = "CREATE INDEX way_tags_index ON way_tags(way_id)";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Creating index of table <way_nodes>... ";
			sql = "CREATE INDEX way_nodes_index ON way_nodes(way_id)";
		    stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Creating index of table <relation_tags>... ";
			sql = "CREATE INDEX relation_tags_index ON relation_tags(rel_id)";
		    stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
			logText = " - Creating index of table <relation_members>... ";
			sql = "CREATE INDEX relation_members_index ON relation_members(rel_id)";
			stmt.executeUpdate(sql);
			Log.info(logText + "Ok!!");
			
		    stmt.close();			
		}
		catch(SQLException se) {
			
			Log.error(logText + "SQL Error: " + se.getMessage());
			
			return false;
		}
		finally {
			
			//finally block used to close resources
		    try {
		    	
		    	if (stmt!=null)
		    		stmt.close();
		    }
		    catch(SQLException se2) {
		    	
		    }		    
		}
		
		try {
			
			mPrepStmtInsertNode = mConn.prepareStatement(mSqlInsertNode);
			mPrepStmtInsertNodeTags = mConn.prepareStatement(mSqlInsertNodeTags);
			
			mPrepStmtInsertWay = mConn.prepareStatement(mSqlInsertWay);
			mPrepStmtInsertWayTags = mConn.prepareStatement(mSqlInsertWayTags);
			mPrepStmtInsertWayNodes = mConn.prepareStatement(mSqlInsertWayNodes);
			
			mPrepStmtInsertRelation = mConn.prepareStatement(mSqlInsertRelation);
			mPrepStmtInsertRelationTags = mConn.prepareStatement(mSqlInsertRelationTags);
			mPrepStmtInsertRelationMembers = mConn.prepareStatement(mSqlInsertRelationMembers);
					
		} catch (SQLException e) {
			
			Log.error("SQL Error in prepareStatement(): " + e.getMessage());
		}
		
		return true;
	}
	
	public synchronized boolean addNode(Node node) {
		
		long nodeId = node.getId();
		
		double lon = node.getLongitude();
		double lat = node.getLatitude();
		
        try {
        	
        	mPrepStmtInsertNode.setLong(1, nodeId);
        	mPrepStmtInsertNode.setDouble(2, lon);
        	mPrepStmtInsertNode.setDouble(3, lat);
        	mPrepStmtInsertNode.executeUpdate();
        }
        catch (SQLException e) {
        	
            Log.error("addNode(): ERROR: "+e.getMessage());
            
            return false;
        }
        
        Collection<Tag> tags=node.getTags();
        
        Iterator<Tag> tagIter=tags.iterator();
        
        while(tagIter.hasNext()) {
        	
        	Tag tag=tagIter.next();
        	
        	String key=tag.getKey();
        	String value=tag.getValue();
    		
    		try {
    			
    			mPrepStmtInsertNodeTags.setLong(1, nodeId);
    			mPrepStmtInsertNodeTags.setString(2, key);
            	mPrepStmtInsertNodeTags.setString(3, value);
            	mPrepStmtInsertNodeTags.executeUpdate();
            }
            catch (SQLException e) {
            	
            	Log.error("addNode() tag: ERROR: "+e.getMessage());
                
                return false;
            }
        }
		
		return true;
	}
	
	public synchronized boolean addWay(Way way) {
		
		long wayId=way.getId();
		 
        try {

        	mPrepStmtInsertWay.setLong(1, wayId);
        	mPrepStmtInsertWay.executeUpdate();

        }
        catch (SQLException e) {
        	
        	Log.error("addWay(): ERROR: "+e.getMessage());
            
            return false;
        }
        
        Collection<Tag> tags=way.getTags();
        
        Iterator<Tag> tagIter=tags.iterator();
        
        while(tagIter.hasNext()) {
        	
        	Tag tag=tagIter.next();
        	
        	String key=tag.getKey();
        	String value=tag.getValue();
		
    		try {
       	
    			mPrepStmtInsertWayTags.setLong(1, wayId);
    			mPrepStmtInsertWayTags.setString(2, key);
            	mPrepStmtInsertWayTags.setString(3, value);
            	mPrepStmtInsertWayTags.executeUpdate();

            }
            catch (SQLException e) {
            	
            	Log.error("addWay() tag: ERROR: "+e.getMessage());
                
                return false;
            }
        }
        
        Collection<WayNode> wayNodes=way.getWayNodes();
        
        Iterator<WayNode> nodesIter=wayNodes.iterator();
        
        int memberSequence=0;
        
        while(nodesIter.hasNext()) {
        	
        	WayNode wayNode=nodesIter.next();
        	
        	long nodeId=wayNode.getNodeId();

    		
    		try {
            	
    			mPrepStmtInsertWayNodes.setLong(1, wayId);
    			mPrepStmtInsertWayNodes.setInt(2, memberSequence);
            	mPrepStmtInsertWayNodes.setLong(3, nodeId);
            	mPrepStmtInsertWayNodes.executeUpdate();
            }
            catch (SQLException e) {
            	
            	Log.error("addWay() wayNode: ERROR: "+e.getMessage());
                
                return false;
            }
    		
    		memberSequence++;
        }
		
		return true;
	}

	public synchronized boolean addRelation(Relation relation) {
		
		long relId=relation.getId();

		 
        try {
        	
        	mPrepStmtInsertRelation.setLong(1, relId);
        	mPrepStmtInsertRelation.executeUpdate();
        }
        catch (SQLException e) {
        	
        	Log.error("addRelation(): ERROR: "+e.getMessage());
            
            return false;
        }
        
        Collection<Tag> tags=relation.getTags();
        
        Iterator<Tag> tagIter=tags.iterator();
        
        while(tagIter.hasNext()) {
        	
        	Tag tag=tagIter.next();
        	
        	String key=tag.getKey();
        	String value=tag.getValue();
        	
    		try {
            	
    			mPrepStmtInsertRelationTags.setLong(1, relId);
    			mPrepStmtInsertRelationTags.setString(2, key);
            	mPrepStmtInsertRelationTags.setString(3, value);
            	mPrepStmtInsertRelationTags.executeUpdate();
            }
            catch (SQLException e) {
            	
            	Log.error("addRelation() tag: ERROR: "+e.getMessage());
                
                return false;
            }
        }
        
        Collection<RelationMember> members=relation.getMembers();
        
        Iterator<RelationMember> memberIter=members.iterator();
        
        int memberSequence=0;
        
        while(memberIter.hasNext()) {
        	
        	RelationMember member=memberIter.next();
        	
        	long memberId=member.getMemberId();
        	String memberRole=member.getMemberRole();
        	
        	int memberType;
        	
        	switch(member.getMemberType()) {
        	
        	case Bound:
        		memberType=0;
        		break;
        		
        	case Node:
        		memberType=1;
        		break;
        		
        	case Way:
        		memberType=2;
        		break;
        		
        	case Relation:
        		memberType=3;
        		break;
        		
        	default:
        		Log.warning("addRelation() member: Unknown relation member type");
        		memberType=-1;
                break;
        	}
    		
    		try {
            	
    			mPrepStmtInsertRelationMembers.setLong(1, relId);
    			mPrepStmtInsertRelationMembers.setInt(2, memberSequence);
            	mPrepStmtInsertRelationMembers.setInt(3, memberType);
            	mPrepStmtInsertRelationMembers.setLong(4, memberId);
            	mPrepStmtInsertRelationMembers.setString(5, memberRole);
            	mPrepStmtInsertRelationMembers.executeUpdate();
            }
            catch (SQLException e) {
            	
            	Log.error("addRelation() member: ERROR: "+e.getMessage());
                
                return false;
            }
    		
    		memberSequence++;
        }
        
        return true;
	}
	
	public void runTest() {
		
		Log.info("Starting runTest()...");
		
		long id=100;
		int version=0;
		
		try {
			mConn.setAutoCommit(false);
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		
		Date timestamp=new Date();
		
		OsmUser user=new OsmUser(0, "user");
		
		CommonEntityData entityData=new CommonEntityData(id, version, timestamp, user, 0);
		
		Node node=new Node(entityData, 0.0, 0.0);
		
		int NUM_NODES=10;
		
		long startTime=System.currentTimeMillis();
		
		for(int i=0; i<NUM_NODES; i++) {
			
			addNode(node);
			
			id++;
			
			node.setId(id);
		}
		
		long elapsedTime=System.currentTimeMillis()-startTime;
		
		String text;
		
		text=String.format(Locale.ENGLISH, "Added %d nodes in %.1f seconds", NUM_NODES, (float)elapsedTime/1000);
        System.out.println(text);
        
        text=String.format(Locale.ENGLISH, "%.1f nodes per second", (float)NUM_NODES/elapsedTime*1000);
        System.out.println(text);
        
        Instant start=Instant.now();
        
        try {
        	
        	System.out.print("Database commit()... ");
			
        	mConn.commit();
			
		} catch (SQLException e) {
			
			System.out.println("Failed(): "+e.getMessage());
		}
        
        Instant end=Instant.now();
        
        text=String.format("Ok!! (Commit took "+Util.timeFormat(start, end)+")");
        
        System.out.println(text);		
        
        System.out.println("runTest() finished!!");
	}
	
	public boolean setAutoCommit(boolean mode) {
		
		String logText = "OsmDatabase::setAutoCommit() to <"+mode+">... ";
		
		try {
			mConn.setAutoCommit(mode);
			
			Log.info(logText + "Ok!!");
		
		} catch (SQLException e) {
			
			Log.error(logText + "ERROR: "+e.getMessage());
			
			return false;
		}
		
		return true;
	}
	
	public boolean commit() {
		
		String logText = "";
		
		try {
			
			logText = "OsmDatabase:commit()... ";
			
			Instant start = Instant.now();
			
			mConn.commit();
			
			Instant end = Instant.now();
			
			logText += String.format("Ok!! (Commit took " + Util.timeFormat(start, end) + ")");
	        
	        Log.info(logText);
			
		} catch (SQLException e) {
			
			Log.error(logText + "Failed!! Error=" + e.getMessage());
			return false;
		}
		
		return true;
	}
	
	public Coord getWayCoord(long wayId) {
		
		Way way = getWayById(wayId);
		
		return getWayCoord(way);
	}
	
	public Coord getWayCoord(Way way) {
		
		if (way == null) {
			
			return null;
		}
		
		List<WayNode> wayNodes = way.getWayNodes();
		
		if (wayNodes.size() == 0) {
			
			return null;
		}
		
		long firstNodeId = wayNodes.get(0).getNodeId();
		
		return getNodeCoord(firstNodeId);
	}
	
	public Coord getRelationCoord(long relationId) {
		
		Relation relation = getRelationById(relationId);
		
		return getRelationCoord(relation);
	}
	
	public Coord getRelationCoord(Relation relation) {
		
		Coord coord = null;
		
		List<RelationMember> members = relation.getMembers();
		
		Iterator<RelationMember> iter = members.iterator();
		
		while(iter.hasNext()) {
			
			RelationMember member = iter.next();
			
			EntityType type = member.getMemberType();
			
			if (type == EntityType.Node) {
				
				coord = getNodeCoord(member.getMemberId());
				
				if (coord != null) {
					
					break;
				}
			}
			else if (type == EntityType.Way) {
				
				coord = getWayCoord(member.getMemberId());
				
				if (coord != null) {
					
					break;
				}
			}
			else if (type == EntityType.Relation) {
				
				coord = getRelationCoord(member.getMemberId());
				
				if (coord != null) {
					
					break;
				}
			}
		}
		
		if (coord == null) {
			
			Log.warning("No valid coord found in <Relation #"+relation.getId()+">");
		}
		
		return coord;
	}
	
	public Node getNodeById(long nodeId) {
		
		Node node=null;
		
		int version=0;
		Date timeStamp=null;
		OsmUser user=null;
		long changesetId=0;
		
		Collection<Tag> tags=getNodeTags(nodeId);
		
		if (tags==null) {
			
			System.out.println("Error while getting tags of node #"+nodeId);
			
			return null;
		}
		
		CommonEntityData entityData=new CommonEntityData(nodeId, version,
				timeStamp, user, changesetId, tags);
		
		Coord coord=getNodeCoord(nodeId);
		
		if (coord!=null)
			node=new Node(entityData, coord.mLat, coord.mLon);
		
		return node;
	}
	
	public Coord getNodeCoord(long nodeId) {
		
		Coord coord=null;
		
		String sql=
				"SELECT\n" + 
				" id,\n" + 
				" lon,\n" + 
				" lat\n" + 
				"FROM\n" + 
				" nodes\n" + 
				"WHERE\n" +
				" id=?";
		
		PreparedStatement pstmt=null;
		
		try {
			pstmt = mConn.prepareStatement(sql);
			pstmt.setLong(1, nodeId);
			
			ResultSet rs=pstmt.executeQuery();
			
			if (!rs.next()) {
				
				Log.warning("getNodeCoord(): ResultSet is empty of node <"+nodeId+">");
				
				coord=null;
			}
			else {
				
				double lon=rs.getDouble("lon");
				double lat=rs.getDouble("lat");
				
				coord=new Coord(lat, lon);
			}
			
			rs.close();
			
			pstmt.close();
			
		} catch (SQLException e) {
			
			System.out.println("Error: "+e.getMessage());
			
			coord=null;
		}
		
		return coord;		
	}
	
	public Collection<Tag> getNodeTags(long nodeId) {
		
		ArrayList<Tag> tags=new ArrayList<Tag>();
		
		String sql=
				"SELECT\n" + 
				" node_id,\n" + 
				" key,\n" + 
				" value\n" + 
				"FROM\n" + 
				" node_tags\n" + 
				"WHERE\n" +
				" node_id=?";
		
		PreparedStatement pstmt=null;
		
		try {
			
			pstmt = mConn.prepareStatement(sql);
			pstmt.setLong(1, nodeId);
			
			ResultSet rs=pstmt.executeQuery();
			
			while(rs.next()) {
				
				String key=rs.getString("key");
				String value=rs.getString("value");
				
				Tag tag=new Tag(key, value);
				
				tags.add(tag);
			}
			
			rs.close();
			
			pstmt.close();
			
		} catch (SQLException e) {
			
			System.out.println("Error: "+e.getMessage());
			
			tags=null;
		}
		
		return tags;
	}
	
	public Way getWayById(long wayId) {
		
		int version=0;
		Date timeStamp=null;
		OsmUser user=null;
		long changesetId=0;
		
		Collection<Tag> tags=getWayTags(wayId);
		
		if (tags==null) {
			
			Log.error("Error while getting tags of way #"+wayId);
			
			return null;
		}
		
		List<WayNode> wayNodes=getWayNodes(wayId);
		
		if (wayNodes==null) {
			
			Log.error("Error while getting members of way #"+wayId);
			
			return null;
		}
		
		CommonEntityData entityData=new CommonEntityData(wayId, version,
				timeStamp, user, changesetId, tags);
		
		Way way=new Way(entityData, wayNodes);
		
		return way;
	}
	
	public Collection<Tag> getWayTags(long wayId) {
		
		ArrayList<Tag> tags=new ArrayList<Tag>();
		
		String sql=
				"SELECT\n" + 
				" way_id,\n" + 
				" key,\n" + 
				" value\n" + 
				"FROM\n" + 
				" way_tags\n" + 
				"WHERE\n" +
				" way_id=?";
		
		PreparedStatement pstmt=null;
		
		try {
			pstmt = mConn.prepareStatement(sql);
			pstmt.setLong(1, wayId);
			
			ResultSet rs=pstmt.executeQuery();
			
			while(rs.next()) {
				
				String key=rs.getString("key");
				String value=rs.getString("value");
				
				Tag tag=new Tag(key, value);
				
				tags.add(tag);
			}
			
			rs.close();
			
			pstmt.close();
			
		} catch (SQLException e) {
			
			System.out.println("Error: "+e.getMessage());
			
			tags=null;
		}
		
		return tags;
	}
	
	public List<WayNode> getWayNodes(long wayId) {
		
		ArrayList<WayNode> wayNodes=new ArrayList<WayNode>();
		
		String sql=
				"SELECT\n" + 
				" way_id,\n" + 
				" sequence,\n" + 
				" node_id\n" + 
				"FROM\n" + 
				" way_nodes\n" + 
				"WHERE\n" +
				" way_id=?\n"+
				"ORDER BY\n"+
				" sequence ASC";
		
		PreparedStatement pstmt=null;
		
		try {
			pstmt = mConn.prepareStatement(sql);
			pstmt.setLong(1, wayId);
			
			ResultSet rs=pstmt.executeQuery();
			
			while(rs.next()) {
				
				Long nodeId=rs.getLong("node_id");
				
				WayNode wayNode=new WayNode(nodeId);
				
				wayNodes.add(wayNode);
			}
			
			rs.close();
			
			pstmt.close();
			
		} catch (SQLException e) {
			
			System.out.println("Error: "+e.getMessage());
			
			wayNodes=null;
		}
		
		return wayNodes;
	}
	
	public Relation getRelationById(long relId) {
		
		int version=0;
		Date timeStamp=null;
		OsmUser user=null;
		long changesetId=0;
		
		Collection<Tag> tags=getRelationTags(relId);
		
		if (tags==null) {
			
			Log.error("Error while getting tags of relation #"+relId);
			
			return null;
		}
		
		List<RelationMember> members=getRelationMembers(relId);
		
		if (members==null) {
			
			Log.error("Error while getting members of relation #"+relId);
			
			return null;
		}
		
		CommonEntityData entityData=new CommonEntityData(relId, version,
				timeStamp, user, changesetId, tags);
		
		Relation relation=new Relation(entityData, members);
		
		return relation;
	}
	
	public Collection<Tag> getRelationTags(long relId) {
		
		ArrayList<Tag> tags=new ArrayList<Tag>();
		
		String sql=
				"SELECT\n" + 
				" rel_id,\n" + 
				" key,\n" + 
				" value\n" + 
				"FROM\n" + 
				" relation_tags\n" + 
				"WHERE\n" +
				" rel_id=?";
		
		PreparedStatement pstmt=null;
		
		try {
			pstmt = mConn.prepareStatement(sql);
			pstmt.setLong(1, relId);
			
			ResultSet rs=pstmt.executeQuery();
			
			while(rs.next()) {
				
				String key=rs.getString("key");
				String value=rs.getString("value");
				
				Tag tag=new Tag(key, value);
				
				tags.add(tag);
			}
			
			rs.close();
			
			pstmt.close();
			
		} catch (SQLException e) {
			
			Log.error("getRelationTags() error: "+e.getMessage());
			
			tags=null;
		}
		
		return tags;
	}
	
	public String getRelationTagValue(long relId, String tagKey) {
		
		Collection<Tag> tags = getRelationTags(relId);
		
		if (tags == null) {
			
			return null;
		}
		
		Iterator<Tag> iter = tags.iterator();
		
		while (iter.hasNext()) {
			
			Tag tag = iter.next();
			
			if (tag.getKey().compareTo(tagKey) == 0) {
				
				return tag.getValue();
			}
		}
		
		Log.debug("Tag key '" + tagKey + "' not found in relation with Id #" + relId);
		
		return null;
	}
	
	public List<RelationMember> getRelationMembers(long relId) {
		
		ArrayList<RelationMember> members=new ArrayList<RelationMember>();
		
		String sql=
				"SELECT\n" + 
				" rel_id,\n" + 
				" sequence,\n" + 
				" member_type,\n" + 
				" member_id,\n" + 
				" member_role\n" + 
				"FROM\n" + 
				" relation_members\n" + 
				"WHERE\n" +
				" rel_id=?\n"+
				"ORDER BY\n"+
				" sequence ASC";
		
		PreparedStatement pstmt=null;
		
		try {
			pstmt = mConn.prepareStatement(sql);
			pstmt.setLong(1, relId);
			
			ResultSet rs=pstmt.executeQuery();
			
			while(rs.next()) {
				
				int memberTypeIndex=rs.getInt("member_type");
				
				EntityType memberType;
				
				switch(memberTypeIndex) {
				
				case 0:
					memberType=EntityType.Bound;
					break;
	        		
	        	case 1:
	        		memberType=EntityType.Node;
	        		break;
	        		
	        	case 2:
	        		memberType=EntityType.Way;
	        		break;
	        		
	        	case 3:
	        		memberType=EntityType.Relation;
	        		break;
	        		
	        	default:
	        		Log.error("getRelationMembers(): Unknown member type index <"+memberTypeIndex+
	        				"> of relation <"+relId+">");
	        		return null;					
				}
				
				Long memberId=rs.getLong("member_id");
				
				String memberRole=rs.getString("member_role");
				
				RelationMember member=new RelationMember(memberId, memberType, memberRole);
				
				members.add(member);
			}
			
			rs.close();
			
			pstmt.close();
			
		} catch (SQLException e) {
			
			System.out.println("Error: "+e.getMessage());
			
			members=null;
		}
		
		return members;
	}
	
	public List<Long> getRelationsIdsByTags(Collection<Tag> tags) {
		
		if (tags==null) {
			Log.warning("getRelationsIdsByTags. tags is null!!");
			return null;
		}
		
		if (tags.isEmpty()) {
			
			Log.warning("getRelationsIdsByTags. tags is empty!!");
			return null;
		}
		
		// Create first query
		String sql=null;
		
		Iterator<Tag> iter=tags.iterator();
		
		while(iter.hasNext()) {
			
			Tag tag=iter.next();
			
			if (sql==null) {
				
				// This is the first tag
				
				sql=
					"SELECT rel_id\n"+ 
					"FROM relation_tags\n"+ 
					"WHERE key='"+tag.getKey()+"' AND value='"+tag.getValue()+"'\n";
			}
			else {
				
				// This is not the first tag. Add query to subqueries...
				
				sql=
					"SELECT rel_id\n"+
					"FROM relation_tags\n"+
					"WHERE rel_id IN (\n"+
					sql+
					")\n"+ 
					"AND key='"+tag.getKey()+"' AND value='"+tag.getValue()+"'\n";
			}
			
		}
		
		ArrayList<Long> relIds=new ArrayList<Long>();
		
		Statement stmt=null;
		
		try {
			stmt = mConn.createStatement();
			ResultSet rs=stmt.executeQuery(sql);
			
			while(rs.next()) {
				
				long relId=rs.getLong("rel_id");
				
				relIds.add(relId);
			}
			
			rs.close();
			
			stmt.close();
			
		} catch (SQLException e) {
			
			Log.error(e.getMessage());
			
			relIds=null;
		}	
		
		return relIds;
	}
	
	
	public List<Long> getRelationsIdsByType(String typeName) {
		
		ArrayList<Long> relIds=new ArrayList<Long>();
		
		PreparedStatement pstmt=null;
		
		String sql=
				"SELECT\n" + 
				" relations.id,\n" + 
				" key,\n" + 
				" value\n" + 
				"FROM\n" + 
				" relations\n" + 
				" INNER JOIN relation_tags ON relations.id = relation_tags.id\n" + 
				"WHERE\n" + 
				" relation_tags.key =\"type\" AND relation_tags.value=?";
		
		try {
			pstmt = mConn.prepareStatement(sql);
			pstmt.setString(1, typeName);
			ResultSet rs=pstmt.executeQuery();
			
			while(rs.next()) {
				
				long relId=rs.getLong("id");
				
				relIds.add(relId);
			}
			
			rs.close();
			
			pstmt.close();
			
		} catch (SQLException e) {
			
			Log.error(e.getMessage());
			
			relIds=null;
		}
				
		return relIds;
	}
	
	public List<Long> filterRelations(List<Long> input, String key, String value) {
		
		Instant start=Instant.now();
		
		List<Long> output = new ArrayList<Long>();
		
		Iterator<Long> iter=input.iterator();
		
		while(iter.hasNext()) {
			
			Long relId=iter.next();
			
			String sql=
					"SELECT\n" + 
					" rel_id,\n" + 
					" key,\n" + 
					" value\n" + 
					"FROM\n" + 
					" relation_tags\n" + 
					"WHERE\n" + 
					" id=? AND key=? AND value=?";
			
			PreparedStatement pstmt=null;
			
			try {
				pstmt = mConn.prepareStatement(sql);
				pstmt.setLong(1, relId);
				pstmt.setString(2, key);
				pstmt.setString(3, value);
				
				ResultSet rs=pstmt.executeQuery();
				
				if (rs.next()) {
					
					output.add(relId);
				}
				
				rs.close();
				
				pstmt.close();
			
			} catch (SQLException e) {
				
				Log.error(e.getMessage());

			}
		}
		
		Instant end=Instant.now();
		
		long time=Duration.between(start, end).toMillis();
		
		Log.debug("Filter took "+time+" ms");
		
		return output;
	}
	
	public void checkHiking(List<Long> relIds) {
		
		Iterator<Long> relIter=relIds.iterator();
		
		while(relIter.hasNext()) {
			
			Long relId=relIter.next();
			
			Relation rel=getRelationById(relId);
			
			if (rel!=null)
				checkHikingRelation(rel);			
		}		
	}
	
	public void checkHikingRelation(Relation relation) {
		
		boolean processed=false;
		
		Collection<Tag> relTags=relation.getTags();
		
		Iterator<Tag> tagIter=relTags.iterator();
		
		while(tagIter.hasNext()) {
			
			Tag tag=tagIter.next();
			
			if (tag.getKey().compareTo("type")==0) {
				
				if (tag.getValue().compareTo("route")==0) {
					
					checkHikingRoute(relation);
					
					processed=true;
					
					break;
				}
				else if (tag.getValue().compareTo("superroute")==0) {
				
					checkHikingSuperRoute(relation);
					
					processed=true;
					
					break;
				}
				else {
					
					Log.warning("Hiking: Relation #"+relation.getId()+" has an incorrect type <"+tag.getValue()+">");
					
					break;
				}
			}				
		}
		
		if (!processed) {
			
			Log.warning("Hiking: Relation #"+relation.getId()+" is not a route or superroute");
		}
	}
	
	public void checkHikingSuperRoute(Relation relation) {
		
		boolean isHikingRoute=false;
		
		Collection<Tag> relTags=relation.getTags();
		
		Iterator<Tag> tagIter=relTags.iterator();
		
		while(tagIter.hasNext()) {
			
			Tag tag=tagIter.next();
			
			if (tag.getKey().compareTo("route")==0) {
				
				if (tag.getValue().compareTo("hiking")==0) {
					
					isHikingRoute=true;
				}
			}
		}
				
		if (!isHikingRoute) {
			
			Log.warning("Hiking: Super Route Relation #"+relation.getId()+" is not a hiking route");
		}
		
		int numberOfRoutes=0;
		
		List<RelationMember> members=relation.getMembers();
		
		for(int pos=0; pos<members.size(); pos++) {
			
			RelationMember member=members.get(pos);
			
			if (!member.getMemberRole().isEmpty()) {
				
				Log.warning("Hiking: Super Route Relation #"+relation.getId()+": Member in pos <"+pos+
						"> does not have an empty role <"+member.getMemberRole()+">");
			}
			
			if (member.getMemberType()==EntityType.Relation) {
				
				Relation routeRel=getRelationById(member.getMemberId());
				
				checkHikingRoute(routeRel);
				
				numberOfRoutes++;				
			}
			else {
				
				Log.warning("Hiking: Super Route Relation #"+relation.getId()+": Member in pos <"+pos+
						"> is not a relation");
			}				
		}
		
		if (numberOfRoutes<1) {
			
			Log.warning("Hiking: Super Route Relation #"+relation.getId()+": Super Route does not have any relation");			
		}
		else if (numberOfRoutes<2) {
			
			Log.warning("Hiking: Super Route Relation #"+relation.getId()+": Super Route only has 1 relation");			
		}
	}
	
	public void checkHikingRoute(Relation relation) {
	
	}
	
	public void saveDatabaseInfo(OsmPbfFile pbf) {
		
		String sqlAddDatabaseInfo = "INSERT INTO db_info(version, creation_date, creation_time," +
				" min_lon, max_lon, min_lat, max_lat, pbf_date, pbf_time) VALUES(?,?,?,?,?,?,?,?,?)";
		try {
			
			PreparedStatement prepStmtAddDatabaseInfo = mConn.prepareStatement(sqlAddDatabaseInfo);
			
			LocalDateTime time = LocalDateTime.now();
			
			String dateString=String.format("%04d-%02d-%02d",
					time.getYear(),
					time.getMonthValue(),
					time.getDayOfMonth());
			
			String timeString=String.format("%02d:%02d:%02d",
					time.getHour(),
					time.getMinute(),
					time.getSecond());
			
			prepStmtAddDatabaseInfo.setString(1, DB_VERSION);
        	prepStmtAddDatabaseInfo.setString(2, dateString);
        	prepStmtAddDatabaseInfo.setString(3, timeString);
        	prepStmtAddDatabaseInfo.setFloat(4, (float) pbf.mMinLon);
        	prepStmtAddDatabaseInfo.setFloat(5, (float) pbf.mMaxLon);
        	prepStmtAddDatabaseInfo.setFloat(6, (float) pbf.mMinLat);
        	prepStmtAddDatabaseInfo.setFloat(7, (float) pbf.mMaxLat);
        	prepStmtAddDatabaseInfo.setString(8, pbf.mFileDateStamp);
        	prepStmtAddDatabaseInfo.setString(9, pbf.mFileTimeStamp);
        	prepStmtAddDatabaseInfo.executeUpdate();
        }
        catch (SQLException e) {
        	
            Log.error("saveDatabaseInfo() SQL Error: " + e.getMessage());
            
            return;
        }		
	}
	
	public void readDatabaseInfo() {
		
		Log.info("Read Database Info...");
		
		String sql=
				"SELECT\n" + 
				" version,\n" + 
				" creation_date,\n" + 
				" creation_time,\n" + 
				" min_lon,\n" + 
				" max_lon,\n" + 
				" min_lat,\n" + 
				" max_lat,\n" + 
				" pbf_date,\n" + 
				" pbf_time\n" + 
				"FROM\n" + 
				" db_info";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = mConn.prepareStatement(sql);
			
			ResultSet rs = pstmt.executeQuery();
			
			while(rs.next()) {
				
				mVersion = rs.getString("version");
				Log.info(" - DB Version: " + mVersion);
				
				mCreationDate = rs.getString("creation_date");
				Log.info(" - DB Creation Date: " + mCreationDate);
				
				mCreationTime = rs.getString("creation_time");
				Log.info(" - DB Creation Time: " + mCreationTime);
				
				mMinLon = rs.getFloat("min_lon");
				Log.info(" - DB Min Lon: " + mMinLon);
				
				mMaxLon = rs.getFloat("max_lon");
				Log.info(" - DB Max Lon: " + mMaxLon);
				
				mMinLat = rs.getFloat("min_lat");
				Log.info(" - DB Min Lat: " + mMinLat);
				
				mMaxLat = rs.getFloat("max_lat");
				Log.info(" - DB Max Lat: " + mMaxLat);
				
				mPbfDateStamp = rs.getString("pbf_date");
				Log.info(" - PBF Date Stamp: " + mPbfDateStamp);
				
				mPbfTimeStamp = rs.getString("pbf_time");
				Log.info(" - PBF Time Stamp: " + mPbfTimeStamp);
			}
			
			rs.close();
			
			pstmt.close();
			
		} catch (SQLException e) {
			
			Log.error("readDatabaseInfo() SQL error: "+e.getMessage());
		}
	}
}
