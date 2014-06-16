/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.gfz_potsdam.datasync;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joda.time.DateTime;



/**
 *
 * @author damian
 */
public class SyncDB {

    Connection conn;
    private static final Logger log =  Logger.getLogger(SyncDB.class.getName());
    public static boolean FILE=true;
    public static boolean DIRECTORY=false;

    public SyncDB() throws ClassNotFoundException, SQLException{
        Class.forName("org.h2.Driver");
    }
  
    public void storeMapping(String path, String id, long local, DateTime server, boolean isfile) throws Exception{
        storeMapping(path,id,local,server.getMillis(),isfile);
    }
    
    public void storeMapping(String path, String id, long local, long server, boolean isfile) throws Exception{

        if (path.isEmpty())
            throw new Exception ("Path should not be empty");
        if (id.isEmpty())
            throw new Exception ("Id should not be empty");
        
        PreparedStatement stat;
        stat = conn.prepareStatement("MERGE INTO filemap (path,escidocid,local,server,isfile) VALUES (?,?,?,?,?)");
        
        System.out.println("MERGE INTO filemap (path,escidocid,local,server) VALUES ("+path+","+id+","+new DateTime(local)+","+new DateTime(server)+","+Boolean.toString(isfile)+")");
        
        stat.setString(1,path);       
        stat.setString(2,id);
        stat.setTimestamp(3, new Timestamp(local));
        stat.setTimestamp(4, new Timestamp(server));
        stat.setBoolean(5, isfile);
        stat.execute();
        stat.close();

    }    
       
    public boolean remoteIsNewer(String path, String id, DateTime server) throws SQLException{
        return remoteIsNewer(path,id,server.getMillis());
    }        
    public boolean remoteIsNewer(String path, String id,long server) throws SQLException{
        PreparedStatement stat;
        stat = conn.prepareStatement("SELECT path FROM filemap WHERE path=? AND escidocid=? AND server<=?");
        stat.setString(1,path);
        stat.setString(2,id);
        stat.setTimestamp(3,new Timestamp(server));            
        ResultSet rs= stat.executeQuery();
        if (rs.next())
            return false;

        return true;
    }        
    public boolean localIsNewer(String path, String id,DateTime local) throws SQLException{
        return localIsNewer(path,id,local.getMillis());
    }        
    public boolean localIsNewer(String path, String id,long local) throws SQLException{
        PreparedStatement stat;
        stat = conn.prepareStatement("SELECT path FROM filemap WHERE path=? AND escidocid=? AND local<=?");
 
        stat.setString(1,path);
        stat.setString(2,id);
        stat.setTimestamp(3,new Timestamp(local));        
        ResultSet rs = stat.executeQuery();
        if (rs.next())        
            return false;
        return true;
    }  
    
    public HashMap<String,String> listEntries(String basedir,String separator) throws Exception{
        
        HashMap<String,String> ret=new HashMap();
        PreparedStatement stat;        
        String dir=basedir+separator+"%";
        String subdir=dir+separator+"%";
        stat=conn.prepareStatement("SELECT path, escidocid FROM filemap WHERE path LIKE ? AND path NOT LIKE ?");
        stat.setString(1,dir);
        stat.setString(2,subdir); 
        ResultSet rs=stat.executeQuery();
        while (rs.next()) {
            ret.put(rs.getString("path"),rs.getString("escidocid"));                    
        }
        return ret;
    }

    public void deleteMapping(String path, String id, String separator) throws SQLException{
        PreparedStatement stat;
        
        boolean removedirectory=false;
        
        stat = conn.prepareStatement("SELECT * FROM filemap WHERE path=? AND escidocid=? AND isfile=?");
        stat.setString(1,path);
        stat.setString(2,id);
        stat.setBoolean(3,SyncDB.DIRECTORY);      
        ResultSet rs = stat.executeQuery();        
        if (rs.next())        
            removedirectory=true;

        System.out.println("DELETE FROM filemap WHERE path="+path+" AND escidocid="+id+")");                
        stat = conn.prepareStatement("DELETE FROM filemap WHERE path=? AND escidocid=?"); 
        stat.setString(1,path);
        stat.setString(2,id);
        stat.execute();        
        
        if (!removedirectory)
            return;
        
        System.out.println("DELETE FROM filemap WHERE path LIKE "+path+separator+"%)");        
        stat = conn.prepareStatement("DELETE FROM filemap WHERE path LIKE ?"); 
        stat.setString(1,path+separator+"%");
        stat.execute();    
        
        
    }     
    
    
    public boolean entryExists(String path, String id) throws SQLException{
        PreparedStatement stat;
        stat = conn.prepareStatement("SELECT path FROM filemap WHERE path=? AND escidocid=?"); 
        stat.setString(1,path);
        stat.setString(2,id);
        ResultSet rs = stat.executeQuery();
        if (rs.next())        
            return true;
       
        return false;        
        
    }    
    public boolean entryExists(String path) throws SQLException{
        PreparedStatement stat;
        stat = conn.prepareStatement("SELECT path FROM filemap WHERE path=?"); 
        stat.setString(1,path);
        ResultSet rs = stat.executeQuery();
        if (rs.next())        
            return true;
       
        return false;        
        
    }      
    
    public void dump() throws SQLException{
        Statement stat;
        stat = conn.createStatement();       
        ResultSet rs= stat.executeQuery("SELECT * FROM filemap");
        while (rs.next()) {
            log.log(Level.INFO," \"{0}\"    \"{1}\"    \"{2}\"    \"{3}\"    \"{4}\"",
                    new Object[]{rs.getString("escidocid"),rs.getTimestamp("local"),rs.getTimestamp("server"),rs.getString("path"),rs.getBoolean("isfile")});    
        }
        stat.close();
        
    }
    
    public void createDB() throws SQLException{
 
        Statement stat = conn.createStatement();
        
        ResultSet rs= stat.executeQuery("SELECT * FROM information_schema.tables WHERE table_name = 'FILEMAP' ");
        
        if (rs.next())
            return;
        
        stat.execute("CREATE TABLE filemap(path VARCHAR(4096), escidocid VARCHAR(255) NOT NULL, local DATETIME NOT NULL, server DATETIME NOT NULL, isfile BOOLEAN NOT NULL, PRIMARY KEY (path, escidocid))");
        stat.close();
    }    

    public void open(String path) throws SQLException{
        conn = DriverManager.getConnection("jdbc:h2:"+path+"/syncDB");        
    }

    public void close() throws SQLException{
        conn.close();        
    }
  
}
