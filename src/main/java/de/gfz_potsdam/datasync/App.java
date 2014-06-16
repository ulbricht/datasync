/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.gfz_potsdam.datasync;

//import de.escidoc.core.client.Authentication;
import de.escidoc.core.client.ContainerHandlerClient;
import de.escidoc.core.client.exceptions.TransportException;
import de.escidoc.core.client.exceptions.application.security.AuthenticationException;
import java.io.File;

/**
 *
 * @author ulbricht
 */
public class App {
    
    
    private static String directory=null;
    private static String container=null;  
    private static String infrastructure=null;
    private static String username=null;
    private static String password=null;
    public  static SyncDB db=null;


    public static void main( String[] args ){
        try
        {
            parseArgs(args);
            if (directory==null || container==null || infrastructure==null || username==null || password==null){
                usage();
                System.exit(1);
            }            
            System.out.println("running");
            db=new SyncDB();
            db.open(directory); 
            db.createDB();
            System.out.println("trying to authenticate ");            
            Authentication auth=new Authentication(infrastructure,username,password);
            
            System.out.println(auth.getHandle()+"\n");
            
            ContainerHandlerClient containerhandler=new ContainerHandlerClient(auth.getServiceAddress());
            containerhandler.setHandle(auth.getHandle());
            
            Datasync sync= new Datasync(auth,containerhandler.retrieve(container),new File(directory));  
            sync.sync();

            db.dump();
            db.close();
            
        }
        catch(TransportException e)
        {
            System.out.println("Error: Problems while talking to eSciDoc-infrastructure: "+e.getMessage());

        }         
        catch(AuthenticationException e)
        {
            System.out.println("Error: Authentication failed");
        }        
        catch(Exception e)
        {
            System.out.println("Error: "+e.getMessage());
            e.printStackTrace();  
        }
    }
    
    protected static void parseArgs(String [] args){
        
        for(int i=0;i<args.length-1;i++){
            if (args[i].equals("-user")){
                username=args[i+1];
            }
            if (args[i].equals("-pass")){
                password=args[i+1];                
            }
            if (args[i].equals("-host")){
                infrastructure=args[i+1];                
            }
            if (args[i].equals("-container")){
                 container=args[i+1];               
            }
            if (args[i].equals("-directory")){
                 directory=args[i+1];               
            }              
                 
        }
    }
    
    protected static void usage (){
        
        System.out.println ("Usage: datasync {options} \n");
        System.out.println ("\t -host \t\t escidoc infrastructure host");        
        System.out.println ("\t -user \t\t escidoc infrastructure username");
        System.out.println ("\t -pass \t\t escidoc infrastructure password");   
        System.out.println ("\t -container \t escidoc infrastructure container that should be synchronized");
        System.out.println ("\t -directory \t local data directory for a database and the local representation of the escidoc container");       
        System.out.println ("\nDatasync synchronizes folders over the internet using an eSciDoc infrastructure as cloud storage.\n");        
        
    }
    
    
}

    
