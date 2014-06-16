/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.gfz_potsdam.datasync;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;

/**
 *
 * @author ulbricht
 */
public class EscidocPackage {
    

    protected HashMap <File,String> components;
    protected HashMap <String, File> pathToFile;
    protected HashMap <String,File> metadata;
    protected boolean isContainer;

       
    public EscidocPackage(){
        components=new HashMap();
        metadata=new HashMap();
        pathToFile=new HashMap();
    }
    public void setIsContainer(){
        isContainer=true;
    }
    public void setIsItem(){
        isContainer=false;        
    }
    public boolean getIsItem(){
        return !isContainer;
    }
    public boolean getIsContainer(){
        return isContainer;
    }    
    public void add(String path,String schema, File file){
        if (schema==null)
            addcomponent(path, file);            
        else
            addmetadata(path, schema, file);
    }
    public void addcomponent(String path, File component){
        components.put(component,null);
        pathToFile.put(path, component);      
    }
    public Collection<File> getComponents(){
        return components.keySet();
    }
    public void addmetadata(String path,String schema, File metafile){
        metadata.put(schema, metafile);
        pathToFile.put(path, metafile);
    }
    public HashMap <String,File> getMetadata(){
        return metadata;
    }
    public Collection<String> getAllFilePaths(){
        Collection<String> ret=pathToFile.keySet();             
        return ret;
    }    
    public File getFileByFilename(String name){
        Collection<File> files=pathToFile.values();
        
        for (File file : files){ 
            if (file.getName().equals(name)){
                return file;
            }
        }
        return null;
    }
    
    private static String getMetadataPrefix(String filename){
            if (!filename.matches("^*\\.*\\.xml"))
                return null;
            
            int end=filename.lastIndexOf(".");
            if (end<0)
                return null;
            int start=filename.substring(0,filename.lastIndexOf(".")).lastIndexOf(".");
            if (start<0)
                return null;
            return filename.substring(0,start);            
    }
    private static String getMetadataSchema(String filename){
            if (!filename.matches("^*\\.*\\.xml"))
                return null;
            
            int end=filename.lastIndexOf(".");
            if (end<0)
                return null;
            int start=filename.substring(0,filename.lastIndexOf(".")).lastIndexOf(".");
            if (start<0)
                return null;
            return filename.substring(start+1,end);            

    }
    private static String getComponentPrefix(String filename){
           
            int end=filename.lastIndexOf(".");
            if (end<0)
                return filename;
           
            return filename.substring(0,end);            

    }
    public static HashMap<EscidocPackage,String> buildPackageList(String basedir, File[] filelist){
        
        HashMap<String,File> metadata=new HashMap();
        HashMap<String,String> metaschema=new HashMap();
        HashMap<String,File> files=new HashMap();        
        HashMap<String,File> directories=new HashMap();
               
        HashMap<String, EscidocPackage> dirpackage=new HashMap();
        HashMap<String, EscidocPackage> filepackage=new HashMap();        
        
        for (File file : filelist){            

            if (file.isDirectory()){
                directories.put(file.getName(), file);
                continue;
            }
            if (getMetadataPrefix(file.getName())!=null && getMetadataSchema(file.getName())!=null){
                metadata.put(file.getName(), file);
                metaschema.put(file.getName(), getMetadataSchema(file.getName()));
                continue;
            }           
            files.put(file.getName(), file);
        }
       
        for (String dirname : directories.keySet()){
            EscidocPackage pkg=new EscidocPackage();
            File dir=directories.get(dirname);
            pkg.addcomponent(basedir+File.separator+dirname, dir );
            pkg.setIsContainer();
            dirpackage.put(dir.getName(), pkg);
        }
        
        for (String filename : files.keySet()){
            String prefix=getComponentPrefix(filename);
            EscidocPackage pkg=filepackage.get(prefix);
            if (pkg==null){
                pkg=new EscidocPackage();
                pkg.setIsItem();
            }
            File file=files.get(filename);
            pkg.addcomponent(basedir+File.separator+filename, file );
            filepackage.put(prefix, pkg);
        } 
        
        
        for (String dirprefix : dirpackage.keySet()){
            for (String metafilename : metadata.keySet()){
                File metafile=metadata.get(metafilename);
                if (metafile.getName().startsWith(dirprefix)){
                    EscidocPackage pkg=dirpackage.get(dirprefix);
                    pkg.addmetadata(basedir+File.separator+metafilename, metaschema.get(metafilename), metafile);
                }                
            }
        }
        for (String fileprefix : filepackage.keySet()){
            for (String metafilename : metadata.keySet()){
                File metafile=metadata.get(metafilename);
                if (metafile.getName().startsWith(fileprefix)){
                    EscidocPackage pkg=dirpackage.get(fileprefix);
                    pkg.addmetadata(basedir+File.separator+metafilename, metaschema.get(metafilename), metafile);
                }                
            }
        }        
       
        HashMap <EscidocPackage,String> result=new HashMap();
        
        for (String dirprefix : dirpackage.keySet()){
            result.put(dirpackage.get(dirprefix), dirprefix);
        }
        for (String fileprefix : filepackage.keySet()){
            result.put(filepackage.get(fileprefix), fileprefix);
        }
        return result;
    }
        
        
    
}
