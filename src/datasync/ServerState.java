/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datasync;

//import de.escidoc.core.client.Authentication;
import de.escidoc.core.client.ContainerHandlerClient;
import de.escidoc.core.client.ItemHandlerClient;
import de.escidoc.core.client.StagingHandlerClient;
import de.escidoc.core.client.exceptions.application.notfound.ResourceNotFoundException;
import de.escidoc.core.resources.common.Result;
import de.escidoc.core.resources.common.TaskParam;
import de.escidoc.core.resources.common.structmap.ContainerRef;
import de.escidoc.core.resources.common.structmap.ItemRef;
import de.escidoc.core.resources.common.structmap.MemberRef;
import de.escidoc.core.resources.common.structmap.StructMap;
import de.escidoc.core.resources.om.GenericVersionableResource;
import de.escidoc.core.resources.om.container.Container;
import de.escidoc.core.resources.om.item.Item;
import de.escidoc.core.resources.om.item.component.Component;
import de.escidoc.core.resources.sb.srw.SearchRetrieveResponseType;
import gov.loc.www.zing.srw.SearchRetrieveRequestType;
import java.io.File;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.FileRequestEntity;
import org.apache.commons.httpclient.methods.PutMethod;
import org.joda.time.DateTime;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;


/**
 *
 * @author ulbricht
 */
public class ServerState {
    
    Authentication auth;
    String topcontainerid;
    HashMap <String,GenericVersionableResource> pathToResource;
    HashMap <String,GenericVersionableResource> idToResource;
    ContainerHandlerClient containerhandler;
    ItemHandlerClient itemhandler;
    StagingHandlerClient staginghandler;
    
    public ServerState (Authentication auth, String topcontainer) throws Exception{
        this.auth=auth;
        this.topcontainerid=topcontainer;
        containerhandler=new ContainerHandlerClient();
        containerhandler.setServiceAddress(auth.getServiceAddress());
        containerhandler.setHandle(auth.getHandle()); 
        itemhandler=new ItemHandlerClient();
        itemhandler.setServiceAddress(auth.getServiceAddress());
        itemhandler.setHandle(auth.getHandle());   
       
        staginghandler=new StagingHandlerClient();
        staginghandler.setServiceAddress(auth.getServiceAddress());
        staginghandler.setHandle(auth.getHandle());            
        idToResource=new HashMap<String,GenericVersionableResource>(); 
        pathToResource=new HashMap<String,GenericVersionableResource>();  
    }
    
    public Container getTopContainer(){
        return (Container)idToResource.get(topcontainerid);
    }

    public GenericVersionableResource getResourceById(String id) throws Exception{
        
        if (!idToResource.containsKey(id))
            throw new ResourceNotFoundException();
        
        return (GenericVersionableResource)idToResource.get(id);
    }    
    
    public GenericVersionableResource getResourceByPath(String path){
        return (GenericVersionableResource)pathToResource.get(path);
    }
    
    private void downloadtree (Container container) throws Exception{
        StructMap sm=container.getStructMap();
        if (sm==null)
            return;
        for (MemberRef ref : sm){            
            if (ref instanceof ItemRef){
                Item item=itemhandler.retrieve(ref.getObjid());
                idToResource.put(item.getObjid(),item);
            }else{                
                Container newcontainer=containerhandler.retrieve(ref.getObjid());
                idToResource.put(newcontainer.getObjid(),newcontainer);                                
                downloadtree(newcontainer);
            }
        }
    }
    
     
    private void indextree (String path, Container container) throws Exception{
    
        StructMap sm=container.getStructMap();
        if (sm==null)
            return;
        for (MemberRef ref : sm){            
            GenericVersionableResource resource;
            String resourcepath;
            
            if (ref instanceof ItemRef){
                Item item=(Item)getResourceById(ref.getObjid());
                for (Component c : item.getComponents()){
                    String filename=c.getProperties().getFileName();
                    resource=item;
                    resourcepath=path+File.separator+filename;
                    pathToResource.put(resourcepath, resource);
                }
            }else if (ref instanceof ContainerRef){
                Container newcontainer=(Container)getResourceById(ref.getObjid());
                String dirname=newcontainer.getProperties().getName();
                resource=newcontainer;
                resourcepath=path+File.separator+dirname;
                indextree(resourcepath,newcontainer);
                pathToResource.put(resourcepath, resource);                
            }
        }
    }
    
    private void fulldownload() throws Exception{
        Container topcontainer=this.containerForceRetrieve(topcontainerid);
        idToResource.clear();        
        idToResource.put(topcontainer.getObjid(),topcontainer);        
        downloadtree(topcontainer);
    }
    private void reindex() throws Exception{
        Container topcontainer=getTopContainer();
        String toppath="/"+topcontainer.getProperties().getName();        
        pathToResource.clear();            
        pathToResource.put(toppath, topcontainer);
        indextree(toppath,topcontainer);        
    }   
    
    public void init() throws Exception{
        fulldownload();
        reindex();
    }
    
    public void update () throws Exception{
        
        String context=getTopContainer().getProperties().getContext().getObjid();
        
        Set<String> ids=idToResource.keySet();
        
        StringBuilder containerids=new StringBuilder();
        StringBuilder itemids=new StringBuilder();

        DateTime lastmodified=null;
        
        for (String id : ids){
            Object o=idToResource.get(id);
            if (o instanceof Container){ 
                Container c=(Container)o;
                if (containerids.length()!=0)
                    containerids.append(" OR ");                
                containerids.append("\"/id\"=\"");
                containerids.append(id);
                containerids.append("\"");
                if (lastmodified==null || lastmodified.isBefore(c.getLastModificationDate()))
                        lastmodified=c.getLastModificationDate();
            }
            if (o instanceof Item){
                Item item=(Item)o;
                if (itemids.length()!=0)
                    itemids.append(" OR ");                
                itemids.append("\"/id\"=\"");
                itemids.append(id);
                itemids.append("\"");               
                 if (lastmodified==null || lastmodified.isBefore(item.getLastModificationDate()))
                        lastmodified=item.getLastModificationDate();               
            }
        }
        
        String containerquery="\"/properties/context/id\"=\""+context+"\" AND ("+containerids+") AND \"/last-modification-date\">\""+lastmodified+"\"";
        SearchRetrieveRequestType containersrr=new SearchRetrieveRequestType();
        containersrr.setQuery(containerquery);
        SearchRetrieveResponseType containersrrp=containerhandler.retrieveContainers(containersrr);    


        String itemquery="\"/properties/context/id\"=\""+context+"\" AND ("+itemids+") AND \"/last-modification-date\">\""+lastmodified+"\"";
        SearchRetrieveRequestType itemsrr=new SearchRetrieveRequestType();
        itemsrr.setQuery(itemquery);
        SearchRetrieveResponseType itemsrrp=null;
        if (itemids.length()!=0)
            itemsrrp=itemhandler.retrieveItems(itemsrr);           
       
         
        
        if (containersrrp.getNumberOfRecords() >0 || (itemsrrp!=null && itemsrrp.getNumberOfRecords() > 0)){
            System.out.println("server updates");
            fulldownload();
            reindex();
        }       
        
    }
    
    public DateTime containerAddMembers (Container parent, Collection<GenericVersionableResource> children) throws Exception{
        TaskParam tp=new TaskParam();
        tp.setLastModificationDate(parent.getLastModificationDate());
        for (GenericVersionableResource child : children)
            tp.addResourceRef(child.getObjid());
        Result r=containerhandler.addMembers(parent.getObjid(), tp);      
  
        for (GenericVersionableResource child : children){
          idToResource.put(child.getObjid(),child);        
        }
        Container ret=containerForceRetrieve(parent.getObjid());
        reindex();
        return ret.getLastModificationDate();      
    }
    
    public DateTime containerRemoveMembers (Container parent, Collection<String> srvdelete)  throws Exception{            
        TaskParam tp=new TaskParam();
        tp.setLastModificationDate(parent.getLastModificationDate());
        for (String child : srvdelete)        
            tp.addResourceRef(child);
        Result r=containerhandler.removeMembers(parent.getObjid(), tp);

        for (String child : srvdelete){
            idToResource.remove(child);
        }
        Container ret=containerForceRetrieve(parent.getObjid());
        reindex();
        return ret.getLastModificationDate();
    }    

    public Container containerForceRetrieve (String containerid)  throws Exception{
        Container  newcontainer=containerhandler.retrieve(containerid);
        idToResource.put(newcontainer.getObjid(),newcontainer);
        return newcontainer;           
    }
    
    public Container containerCreate (Container container)  throws Exception{
        Container  newcontainer=containerhandler.create(container);
        idToResource.put(newcontainer.getObjid(),newcontainer);
        return newcontainer;        
    }
    public Container containerUpdate (Container container)  throws Exception{
        Container  newcontainer=containerhandler.create(container);
        idToResource.put(newcontainer.getObjid(),newcontainer);
        return newcontainer;         
    }    
    public Item itemCreate (Item item) throws Exception{
        Item  newitem=itemhandler.create(item);
        idToResource.put(newitem.getObjid(),newitem);
        return newitem;
        
    }    
    public Item itemUpdate (Item item) throws Exception{
        Item newitem=itemhandler.update(item); 
        idToResource.put(newitem.getObjid(),newitem);        
        return newitem;        
    }
    
    public URL uploadFile (File file) throws Exception{ 
        
        HttpClient client=new HttpClient();
        PutMethod put = new PutMethod(this.auth.getServiceAddress()+"st/staging-file");
        put.setRequestEntity(new FileRequestEntity(file,"application/octet-stream"));
        put.setRequestHeader("Cookie", "escidocCookie="+auth.getHandle());
      
        int responseCode =client.executeMethod(put);
        
        if (responseCode <200 || responseCode >299)
            throw new  Exception (String.format("problems uploading file %s %d",file.getAbsolutePath(),responseCode));
        
        Element e=Util.StringToDocumentElement(put.getResponseBodyAsString());
        
        String base=e.getAttribute("xml:base");
        String href=e.getAttributeNS(e.lookupNamespaceURI("xlink"),"href");

        return new URL(base+href);
        
    }
    
    @Override
    public String toString(){
        StringBuilder ret;
        ret=new StringBuilder();
        
        Set<String> ids=pathToResource.keySet();
        for (String path : ids){
            GenericVersionableResource rc=(GenericVersionableResource)pathToResource.get(path);
            ret.append(path);
            ret.append(" -> ");
            ret.append(rc.getObjid());
            ret.append("\n");
        }
        
        return ret.toString();
    }
     
  
    
}
