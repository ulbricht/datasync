/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.gfz_potsdam.datasync;
//import de.escidoc.core.client.Authentication;
import de.escidoc.core.client.exceptions.application.notfound.ResourceNotFoundException;
import de.escidoc.core.resources.common.reference.Reference;
import de.escidoc.core.resources.common.reference.ContentModelRef;
import de.escidoc.core.resources.common.reference.ContextRef;
import de.escidoc.core.resources.common.MetadataRecord;
import de.escidoc.core.resources.common.MetadataRecords;
import de.escidoc.core.resources.common.structmap.ContainerMemberRef;
import de.escidoc.core.resources.common.structmap.ItemMemberRef;
import de.escidoc.core.resources.common.structmap.MemberRef;
import de.escidoc.core.resources.common.structmap.StructMap;
import de.escidoc.core.resources.VersionableResource;
import de.escidoc.core.resources.om.container.Container;
import de.escidoc.core.resources.om.item.Item;
import de.escidoc.core.resources.om.item.component.Component;
import de.escidoc.core.resources.om.item.component.Components;
import de.escidoc.core.resources.om.item.component.ComponentContent;
import de.escidoc.core.resources.om.item.component.ComponentProperties;
import de.escidoc.core.resources.om.item.StorageType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.io.InputStreamReader;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.joda.time.DateTime;
import org.w3c.dom.Element;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ulbricht
 */
public class Datasync {

    /**
     * @param args the command line arguments
     */

    private ServerState srv;
 
    private Authentication auth;
    private File directory;
    private String contentmodel;
    private String context;
    
    private static final Logger log =  Logger.getLogger(Datasync.class.getName());
       
    public Datasync(Authentication auth, Container topcontainer,File directory) throws Exception{
            
        this.auth=auth;
        this.directory=directory;
        context=topcontainer.getProperties().getContext().getObjid();
        contentmodel=topcontainer.getProperties().getContentModel().getObjid();
    

        srv=new ServerState(auth, topcontainer.getObjid());
                    
    }
    
    public void sync() throws Exception{
        
        srv.init();

        InputStreamReader r= new InputStreamReader(System.in);
        int i=0;
        while (!r.ready()){
            Thread.sleep(1000);
            try{
                log.log(Level.INFO,"Pass {0}",new Object[]{i});
                srv.init();
                srv.update();   
                traverseServerAndDownload(new String(),srv.getTopContainer());
                traverseServerAndDeleteLocal(new String(), srv.getTopContainer());
                traverseLocalAndUpload(new String(),srv.getTopContainer().getProperties().getName());
                srv.update();
                traverseLocalAndDeleteRemote(new String(),srv.getTopContainer().getProperties().getName());
                i++;
            }catch (ResourceNotFoundException e){
                System.out.println("Resource not found - Server state changed");
                e.printStackTrace();
            }
        }
    }
    
    public void traverseLocalAndDeleteRemote(String basedir, String dir) throws Exception{

        Container parent=(Container)srv.getResourceByPath(basedir+File.separator+dir);
        
        File currentdir=new File(directory+basedir+File.separator+dir);
        
        File[] content=currentdir.listFiles(new NoHiddenEntityFilter());
        
        HashSet<String> entitylist=new HashSet();
 
        String thebasedir=basedir+File.separator+dir;
            
        if (content!=null){
            for (File entity : content){
                entitylist.add(thebasedir+File.separator+entity.getName());
            }            
            syncDeletedToRemote(thebasedir,entitylist,parent);            
        }         
        
        File[] dircontent=currentdir.listFiles(new NoHiddenDirectoriesFilter());
        if (dircontent!=null){        
            for (File entry : dircontent){
                traverseLocalAndDeleteRemote(thebasedir,entry.getName());
            }
        }         
    }
    
    public void traverseServerAndDeleteLocal(String basedir, Container container) throws Exception{
  
        if (basedir.isEmpty()){
            Container c=downloadContainer(basedir,new ContainerMemberRef(container.getObjid()));
            if (c==null)
                return;
        }
        
        Container parent=container;
        
        StructMap sm=container.getStructMap();
        
        if (sm!=null){
            
            String thebasedir=basedir+File.separator+container.getProperties().getName();  
            HashMap <String,MemberRef> objects=new HashMap();
            
            for (MemberRef m : sm)
                objects.put(m.getObjid(),m);

            syncRemoteDeletedToLocal(thebasedir,objects);
            
            for (MemberRef m : sm){   
                if (m instanceof ContainerMemberRef){
                    Container c=downloadContainer(thebasedir, (ContainerMemberRef)m);
                    if (c!=null)
                        traverseServerAndDeleteLocal(thebasedir,c);
                }       
            }
        }          
        
    }
    
    
    public void traverseServerAndDownload(String basedir, Container container) throws Exception{
        
        if (basedir.isEmpty()){
            Container c=downloadContainer(basedir,new ContainerMemberRef(container.getObjid()));
            if (c==null)
                return;
        }
        
        Container parent=container;
        
        StructMap sm=container.getStructMap();
      
        if (sm!=null){
            for (MemberRef m : sm){
                
                String dirname=basedir+File.separator+container.getProperties().getName();
                
                if (m instanceof ItemMemberRef){
                    downloadItemFiles(dirname, (ItemMemberRef)m, parent);
                }else if (m instanceof ContainerMemberRef){
                    Container c=downloadContainer(dirname, (ContainerMemberRef)m);
                    if (c!=null)
                        traverseServerAndDownload(dirname,c);
                }       
            }
        }       
    }
    
    private void traverseLocalAndUpload (String basedir, String dir) throws Exception{

        
        Container parent=(Container)srv.getResourceByPath(basedir+File.separator+dir);     
        
        File currentdir=new File(directory+basedir+File.separator+dir);
 
        String thebasedir=basedir+File.separator+dir;        

        File[] dircontent=currentdir.listFiles(new NoHiddenDirectoriesFilter());        
        File[] filecontent=currentdir.listFiles(new NoHiddenEntityFilter());
        
        if (filecontent!=null){        
    
            HashMap<EscidocPackage,String> pkglist=EscidocPackage.buildPackageList(thebasedir, filecontent);
            ArrayList<VersionableResource> resourcelist=new ArrayList();
                
            for (EscidocPackage pkg : pkglist.keySet()){
                String resourcename=pkglist.get(pkg);
                VersionableResource rc;
                if (pkg.getIsItem()){
                    rc=uploadItemPackage(thebasedir,resourcename,pkg);     
                }else{
                    rc=uploadContainerPackage(thebasedir,resourcename,pkg);                    
                }
                if (rc!=null)
                    resourcelist.add(rc);
            }             
            addContainerMembers(thebasedir, parent, resourcelist);
            
        }         
        

        if (dircontent!=null){        
            for (File entry : dircontent){
                traverseLocalAndUpload(thebasedir,entry.getName());
            }
        }                 
    }   

    public void syncDeletedToRemote(String basedir,HashSet<String> entities, Container parent) throws Exception{
        
        //delete in infrastructure container members, where there is no local file but it is in our database
        
        if (parent==null)
            return;    
        
        HashMap<String, String> syncedfiles=App.db.listEntries(basedir,File.separator); 
        HashMap<String,String> srvdelete=new HashMap();
        
        HashMap<String,Integer> pathToIdCount=new HashMap();
        for (String path : syncedfiles.keySet()){
            String id=syncedfiles.get(path);
            Integer count=pathToIdCount.get(id);
            if (count==null)
                count=new Integer(1);
            else
                count=new Integer(count.intValue()+1);
            pathToIdCount.put(id, count);
        }
        
        for (String path : syncedfiles.keySet()){
            String id=syncedfiles.get(path);
            Integer count=pathToIdCount.get(id);
            if (!entities.contains(path)){
                if (count.intValue()<=1){
                    srvdelete.put(path,id);
                    log.log(Level.INFO,"Remote delete: {0} {1}",new Object[]{path,id});
                }   
            }
        }
        //items with more components
        
        if (!srvdelete.isEmpty()){

            srv.containerRemoveMembers(parent,srvdelete.values());
            for (String path : srvdelete.keySet()){
                App.db.deleteMapping(path, srvdelete.get(path),File.separator);

            }
            File parentdir=new File(directory+File.separator+basedir);
            App.db.storeMapping(basedir, parent.getObjid(), parentdir.lastModified(), parent.getLastModificationDate(),SyncDB.DIRECTORY);
       
        }        
            
    } 

    public void syncRemoteDeletedToLocal(String basedir, HashMap<String,MemberRef> objects) throws Exception{
        
        //delete local files, that exist, were downloade/uploaded, did not change and are now missing on server.
        HashMap<String, String> syncedfiles=App.db.listEntries(basedir,File.separator);
        Set <String> syncedfilepath=syncedfiles.keySet();
        
        HashMap <String,String> deletecandidates=new HashMap();
        
        for (String path : syncedfilepath){
            String objid=(String)syncedfiles.get(path);
            if (!objects.containsKey(objid)){
                   deletecandidates.put(path,objid);
            }else{
               MemberRef mref=(MemberRef)objects.get(objid);
               if (mref instanceof ItemMemberRef){
                   Item item=(Item)srv.getResourceById(mref.getObjid());
                   boolean found=false;
                   for (Component c : item.getComponents()){
                       String componentpath=basedir+File.separator+c.getProperties().getFileName();
                       if (path.equals(componentpath))
                           found=true;
                   }
                   if (!found )
                     deletecandidates.put(path,objid);  
               }
            }                      
        }

        for (String deletefilepath : deletecandidates.keySet()){
            File deletefile=new File(directory+File.separator+deletefilepath);
            String objid=(String)deletecandidates.get(deletefilepath);            
            if (App.db.localIsNewer(deletefilepath, objid, deletefile.lastModified()))
                continue;
            
            if (deletefile.exists()) {
                Util.delTree(deletefile);
                App.db.deleteMapping(deletefilepath, objid,File.separator); 
                log.log(Level.INFO,"Deleting Local : {0}",new Object[]{deletefile.getAbsolutePath()});                 
            }
           
        }

        
    }
    
    private void  downloadItemFiles(String basedir, ItemMemberRef ref, Container parent) throws Exception{

        Item item=(Item)srv.getResourceById(ref.getObjid());
        
        for (MetadataRecord mdr: item.getMetadataRecords()){
            if (mdr.getName().equals("escidoc"))
                continue;
           log.log(Level.WARNING,"FIXME - download of Metadata not yet done");            
        }
        
        File syncdir=new File(directory+File.separator+basedir);
        
        for (Component component : item.getComponents()){

            String filename=component.getProperties().getFileName();
            File syncfile=new File(directory+File.separator+basedir+File.separator+filename);

            StringBuilder suffix=new StringBuilder(); 
            
            if (syncfile.exists()){
                if (App.db.localIsNewer(basedir+File.separator+filename, item.getObjid(), syncfile.lastModified())){                
                    //local and remote file have changed
                    suffix.append("-");
                    suffix.append(item.getObjid());
                    suffix.append(":");                
                    suffix.append(item.getProperties().getVersion().getNumber());                
                    suffix.append("-");
                    DateTime mdate=new DateTime(item.getLastModificationDate());
                    suffix.append(mdate.toString("YYYYMMdd-HHmmss"));                
                    syncfile=new File(directory+File.separator+basedir+File.separator+filename+suffix.toString());            
                }else if(!App.db.remoteIsNewer(basedir+File.separator+filename, item.getObjid(), item.getLastModificationDate())){
                    // do not download we downloaded before
                    continue; 
                }
            }else{
                //file was downloaded and locally deleted
                if (App.db.entryExists(basedir+File.separator+filename,item.getObjid()))
                    continue;                
            }                      
           
            HttpClient client=new HttpClient();
            String url=auth.getServiceAddress()+component.getContent().getXLinkHref().replaceAll("^/","");
            GetMethod method=new GetMethod(url);
            method.setRequestHeader("Cookie", "escidocCookie="+auth.getHandle());                        

            try{
                log.log(Level.INFO,"downloading : {0}",new Object[]{syncfile.getAbsolutePath()});                            
                
                client.executeMethod(method);
                InputStream is=method.getResponseBodyAsStream();                              
                FileOutputStream file=new FileOutputStream(syncfile);
                byte[] buffer=new byte[4096];
                int offset=0;
                int len;
                while ((len=is.read(buffer, 0, buffer.length))!=-1){
                    file.write(buffer, 0, len);
                }
                file.flush();                
                file.getFD().sync();
                file.close(); 
                if (suffix.length()==0){
                    App.db.storeMapping(basedir+File.separator+syncfile.getName(), item.getObjid(), syncfile.lastModified(), item.getLastModificationDate(),SyncDB.FILE);
                    App.db.storeMapping(basedir, parent.getObjid(), syncdir.lastModified(), parent.getLastModificationDate(),SyncDB.DIRECTORY);
                }
            }finally{
                method.releaseConnection();
            }
        }
    }
    
    private Container downloadContainer(String basedir, ContainerMemberRef ref) throws Exception{
        Container container=(Container)srv.getResourceById(ref.getObjid());
        String name=container.getProperties().getName();
        File dir=new File(directory+File.separator+basedir+File.separator+name);
        
        //file was locally deleted
        if (!dir.exists() && App.db.entryExists(basedir+File.separator+name, ref.getObjid()))
            return null;
        //directory content changed
        if (dir.exists() && App.db.localIsNewer(basedir+File.separator+name, container.getObjid(), dir.lastModified()))
            return container;
        
        //directory is new or remote content changed
       
        if (dir.exists())
            return container;
        
        dir.mkdir();
        
        App.db.storeMapping(basedir+File.separator+name, container.getObjid(), dir.lastModified(), container.getLastModificationDate(),SyncDB.DIRECTORY);

        log.log(Level.INFO,"downloaded container: {0}",new Object[]{dir.getAbsolutePath()});
        
        return container;
    } 
    
    private Container uploadContainerPackage(String basedir,String dirname, EscidocPackage pkg) throws Exception {
        
        Container container=(Container)srv.getResourceByPath(basedir+File.separator+dirname);
        
        if (container!=null){
            if (!(container instanceof Container))
                  throw new Exception("trying to syc a directory over a file "+basedir+File.separator+dirname);      
        
           boolean newfiles=false;
            
           for (String filepath : pkg.getAllFilePaths()){
               
               //handle only metadata updates here
               if (newfiles)
                  break; 
                       
               File file=new File(directory+filepath);
               if (file.isFile() && App.db.localIsNewer(directory+filepath,container.getObjid(),file.lastModified())){
                   newfiles=true;
               }
           }
           
           if (!newfiles)
                return container;
            
        }else{
            
           for (String filepath : pkg.getAllFilePaths()){
               if (App.db.entryExists(filepath))
                   return null;
           }            
           container=new Container();  
           container.getProperties().setContentModel(new ContentModelRef (contentmodel));
           container.getProperties().setContext(new ContextRef(context));          
           
        }
        


//        MetadataRecord md=new MetadataRecord();

        MetadataRecords mds=new MetadataRecords();
        mds.add(Util.getDefaultMDRecord(dirname));
        
        HashMap<String,File> meta=pkg.getMetadata();
        Set<String> metaNames=meta.keySet();
        for (String metaName : metaNames){
            File metaFile=(File)meta.get(metaName);
            mds.add(Util.ReadMDRecord(metaName,metaFile));                
        }
        container.setMetadataRecords(mds);  
        Container newcontainer;
        
        if (container.getObjid()==null)
            newcontainer=srv.containerCreate(container);
        else
            newcontainer=srv.containerUpdate(container);
        
        File dir=new File(directory+File.separator+basedir+File.separator+dirname);
        
        App.db.storeMapping(basedir+File.separator+dirname, newcontainer.getObjid(), dir.lastModified(), newcontainer.getLastModificationDate(),SyncDB.DIRECTORY);
        
        log.log(Level.INFO,"uploaded container: {0}",new Object[]{dir.getAbsoluteFile()});  
             
        for (File uploadmeta : pkg.getMetadata().values()){
            log.log(Level.INFO,"uploaded : {0}",new Object[]{uploadmeta.getAbsolutePath()});            
            App.db.storeMapping(basedir+File.separator+uploadmeta.getName(), newcontainer.getObjid(), uploadmeta.lastModified(), newcontainer.getLastModificationDate(),SyncDB.FILE);            
        }       
        return newcontainer;

    }
    
    private void addContainerMembers(String parentdir, Container parent, ArrayList<VersionableResource> children) throws Exception{
        if (parent==null || children==null)
            return;

        HashMap<String,VersionableResource> ids=new HashMap();

        for (VersionableResource child : children){
            ids.put(child.getObjid(),child);
        }
        //do not add twice
        if (parent.getStructMap()!=null){
            for (MemberRef m : parent.getStructMap()){
                if (ids.containsKey(m.getObjid()))
                    ids.remove(m.getObjid());
            }
        }
        
        if (ids.isEmpty())
            return;
        
        
        DateTime r=srv.containerAddMembers(parent, ids.values());

        File dir=new File(directory+File.separator+parentdir);

        App.db.storeMapping(parentdir, parent.getObjid(), dir.lastModified(), r, SyncDB.DIRECTORY);
        log.log(Level.INFO,"added members : {0}",new Object[]{dir.getAbsoluteFile()}); 
            
    }
    

    
    private Item uploadItemPackage(String basedir, String itemname, EscidocPackage pkg) throws Exception{
        
        VersionableResource rc=null;
        
        for (String filepath : pkg.getAllFilePaths()){
            rc=(VersionableResource)srv.getResourceByPath(filepath); 
            
            if (rc!=null)
                break;
        }
 
        Item item;
        if (rc != null){
            if (!(rc instanceof Item))
                throw new Exception("trying to syc a file over a directory "+basedir+File.separator+pkg.getAllFilePaths().toString());
            
            item=(Item)rc;
            
            boolean newfiles=false;
            
            for (String filepath : pkg.getAllFilePaths()){
                File file=new File(directory+filepath);
                if (App.db.localIsNewer(directory+filepath,item.getObjid(),file.lastModified()) || !file.exists())
                    newfiles=true;
            }
            if (!newfiles  && item.getComponents().size()==pkg.getComponents().size())
                return item;
        }else{
            //deleted files are still in DB and can not be find as item 
            for (String filepath : pkg.getAllFilePaths()){
                if (App.db.entryExists(filepath))
                    return null;               
            }            
            
            
            item=new Item();
            item.getProperties().setContentModel(new ContentModelRef(contentmodel));
            item.getProperties().setContext(new ContextRef(context));
            MetadataRecords mds=new MetadataRecords();
            mds.add(Util.getDefaultMDRecord(itemname));
            HashMap<String,File> meta=pkg.getMetadata();
            Set<String> metaNames=meta.keySet();
            for (String metaName : metaNames){
                File metaFile=(File)meta.get(metaName);
                mds.add(Util.ReadMDRecord(metaName,metaFile));                
            }
            item.setMetadataRecords(mds);
        }
        
        Components oldcomponents=item.getComponents();
        
        if (oldcomponents==null)
            oldcomponents=new Components();
       
        HashMap<String,String> delComponentsIds=new HashMap();
        for (Component component : oldcomponents){
            File file=pkg.getFileByFilename(component.getProperties().getFileName());
            if (file==null || App.db.localIsNewer(basedir+File.separator+file.getName(),item.getObjid(),file.lastModified()))
                delComponentsIds.put(component.getObjid(),basedir+File.separator+component.getProperties().getFileName());
        }
        
        //components.del(id) does not seem to work
        boolean componentchanged=false;        
        Components components=new Components();
        for (Component component : oldcomponents){
            if (!delComponentsIds.containsKey(component.getObjid())){
                components.add(component);           
            }else{
                    componentchanged=true;                
            }
        }
        

        for (File uploadcomponent : pkg.getComponents()){
            if (item.getObjid()==null){
                    components.add(uploadComponent(uploadcomponent));
                    componentchanged=true;
            }else{
                String componentpath=basedir+File.separator+uploadcomponent.getName();
                if (App.db.localIsNewer(componentpath,item.getObjid(),uploadcomponent.lastModified()) || !App.db.entryExists(componentpath, item.getObjid())){
                    components.add(uploadComponent(uploadcomponent)); 
                    componentchanged=true;
                }
            }
        }
        
        if (!componentchanged)
            return item;
        
        item.setComponents(components);
         
        Item newitem;
        
        if (item.getObjid()==null)
            newitem=srv.itemCreate(item);
        else
            newitem=srv.itemUpdate(item);
        
        log.log(Level.INFO,"uploaded item: {0}",new Object[]{newitem.getObjid()});       
        
        for (String deletedid : delComponentsIds.keySet()){
            App.db.deleteMapping(delComponentsIds.get(deletedid), item.getObjid(),File.separator);
        }
        
        for (File uploadcomponent : pkg.getComponents()){                      
            App.db.storeMapping(basedir+File.separator+uploadcomponent.getName(), newitem.getObjid(), uploadcomponent.lastModified(), newitem.getLastModificationDate(),SyncDB.FILE);            
        }
        
        for (File uploadmeta : pkg.getMetadata().values()){                       
            App.db.storeMapping(basedir+File.separator+uploadmeta.getName(), newitem.getObjid(), uploadmeta.lastModified(), newitem.getLastModificationDate(),SyncDB.FILE);            
        }
        
        return newitem;
    }
    
    public Component uploadComponent(File file) throws Exception{   
        
         URL uploadurl=srv.uploadFile(file);

         ComponentContent content=new ComponentContent();
         content.setXLinkHref(uploadurl.toString());
         content.setStorageType(StorageType.INTERNAL_MANAGED);
         Component component=new Component();
         component.setContent(content);
         component.setProperties(new ComponentProperties());
         component.getProperties().setVisibility("public");
         component.getProperties().setContentCategory("semi-structural");
         component.getProperties().setMimeType("application/octet-stream");

         MetadataRecord componentmd = new MetadataRecord("escidoc");

         Element componentelement=Util.StringToDocumentElement("<file xmlns=\"http://purl.org/escidoc/metadata/profiles/0.1/file\">\n" +
         "    <dc:title xmlns:dc=\"http://purl.org/dc/elements/1.1/\">"+file.getName()+"</dc:title>\n" +
         "    <dc:description xmlns:dc=\"http://purl.org/dc/elements/1.1/\"></dc:description>\n" +
         "    <dc:format xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:dcterms=\"http://purl.org/dc/terms/\" xsi:type=\"dcterms:IMT\">application/octet-stream</dc:format>\n" +
         "    <dcterms:extent xmlns:dcterms=\"http://purl.org/dc/terms/\">"+file.length()+"</dcterms:extent>\n" +
         "    <dcterms:dateCopyrighted xmlns:dcterms=\"http://purl.org/dc/terms/\"></dcterms:dateCopyrighted>\n" +
         "    <dc:rights xmlns:dc=\"http://purl.org/dc/elements/1.1/\"></dc:rights>\n" +
         "    <dcterms:license xmlns:dcterms=\"http://purl.org/dc/terms/\"></dcterms:license>\n" +
         "</file>");

         componentmd.setContent(componentelement);

         log.log(Level.INFO,"uploaded : {0}",new Object[]{file.getAbsolutePath()});           
         MetadataRecords componentmds=new MetadataRecords();
         componentmds.add(componentmd);
         component.setMetadataRecords(componentmds);
         return component;
     }    
            
}
