/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.gfz_potsdam.datasync;

import de.escidoc.core.resources.common.MetadataRecord;
import de.escidoc.core.resources.om.container.Container;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.IUnmarshallingContext;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

/**
 *
 * @author ulbricht
 */
public class Util {
    
    private static final Charset DEFAULT_ENCODING = Charset.forName("UTF8");
  
    public static void delTree(File file) throws Exception{
    
        if (file.isFile()){
            file.delete();
            return;
        }
        if (file.isDirectory()){
            for (File f : file.listFiles())
                delTree(f);
        }
        file.delete();
    }
    
    public static List<String> ExecCmd (String directory, String cmd) throws Exception{
        
        ArrayList<String> ret=new ArrayList<String>();
        BufferedReader reader;        
        Runtime rt = Runtime.getRuntime();   
        File dir=new File (directory);
        Process p=rt.exec(cmd,null,dir);
        p.waitFor();
        int exitcode=p.exitValue();

        InputStream is;
        


        is=p.getInputStream();        
        reader=new BufferedReader(new InputStreamReader(is, DEFAULT_ENCODING));
        for (String line=reader.readLine(); line !=null ; line=reader.readLine())
          ret.add(line);
        reader.close();
        is=p.getErrorStream();
        reader=new BufferedReader(new InputStreamReader(is, DEFAULT_ENCODING));
        for (String line=reader.readLine(); line !=null ; line=reader.readLine())
          ret.add(line);
        reader.close();
        
        if (exitcode!=0){
            StringBuilder sb=new StringBuilder();
            for (String text : ret){
                sb.append(text);
            }
            throw new Exception(sb.toString());
        }

        return ret;   
    
    }    
    
    public static void debugXML(Object o) throws Exception{
        IBindingFactory jc = BindingDirectory.getFactory(o.getClass());
        IMarshallingContext marshaller = jc.createMarshallingContext();
        ByteArrayOutputStream os=new ByteArrayOutputStream();
        marshaller.marshalDocument(o, "UTF-8", null,os);
        System.out.println(os.toString());   
    }
    
    public static Element StringToDocumentElement(String string) throws Exception{
        StringReader sr=new StringReader(string);
        DocumentBuilderFactory dbf=DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new InputSource(sr));            
        return doc.getDocumentElement();
        
    }
    public static Element FileToDocumentElement(File file) throws Exception{
        FileInputStream fis=new FileInputStream(file);
        DocumentBuilderFactory dbf=DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new InputSource(fis));            
        return doc.getDocumentElement();
        
    }     
    public static Object unmarshall(Element elem, Class theclass ) throws Exception{
        TransformerFactory tfactory = TransformerFactory.newInstance();
        Transformer transformer = tfactory.newTransformer();
        StringWriter result=new StringWriter();
        StreamResult output = new StreamResult(result);
//FIXME getFirstChild may be wrong
        DOMSource source = new DOMSource(elem);
        transformer.transform(source, output);
        StringReader sr=new StringReader(result.toString());  
        IBindingFactory jc = BindingDirectory.getFactory(Container.class);
        IUnmarshallingContext unmarshaller = jc.createUnmarshallingContext();

        return unmarshaller.unmarshalDocument(sr);
    }    
    
    public static MetadataRecord getDefaultMDRecord(String dctitle) throws Exception{
        MetadataRecord md=new MetadataRecord("escidoc");
        Element element=Util.StringToDocumentElement("<?xml version=\"1.0\"?>"
        + "<resource xmlns=\"http://example.org/pmdsimpledc/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\n" +
          "  <dc:title xmlns:dc=\"http://purl.org/dc/elements/1.1/\">"+dctitle+"</dc:title>\n" +
          "  <dc:date xmlns:dc=\"http://purl.org/dc/elements/1.1/\"></dc:date>\n" +
          "  <dc:creator xmlns:dc=\"http://purl.org/dc/elements/1.1/\"></dc:creator>\n" +
          "</resource>");

        md.setContent(element);
        return md;
    }
    
    public static MetadataRecord ReadMDRecord(String metaName,File metaFile) throws Exception{
        
        MetadataRecord md=new MetadataRecord(metaName);
        Element element=FileToDocumentElement(metaFile);
        md.setContent(element);
        return md;
    }
            

}
