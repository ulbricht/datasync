/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.gfz_potsdam.datasync;

import java.nio.charset.Charset;


import java.net.URL;
import java.net.HttpURLConnection;

import java.net.URLEncoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.MalformedURLException;

import java.net.ProtocolException;
import java.io.InputStream;
import java.util.HashMap;

import de.escidoc.core.client.exceptions.application.security.AuthenticationException;

/**
 *
 * @author ulbricht
 */
public class Authentication {
     private static final Charset DEFAULT_ENCODING = Charset.forName("UTF8");
     private String service;
     private HashMap <String, String> cookies;
     private String handle;
     
     public Authentication(String service,String username,String password) throws Exception{
         
         this.service=service;
         this.cookies=new HashMap<String,String>();
         StringBuffer ret=new StringBuffer();
         
         String emptybody="";       
         httpRequest(service+"/aa/login?target=",emptybody.getBytes(),ret,"GET");
         
         String body=String.format("j_username=%s&j_password=%s",URLEncoder.encode(username,DEFAULT_ENCODING.name()),URLEncoder.encode(password,DEFAULT_ENCODING.name()));         
         httpRequest(service+"/aa/j_spring_security_check",body.getBytes(),ret,"POST");
         
         httpRequest(service+"/aa/login?target=",emptybody.getBytes(),ret,"GET"); 
         
         if (cookies.get("escidocCookie")==null)
             throw new AuthenticationException(1,"authentication failed","","");
         handle=cookies.get("escidocCookie");
         cookies.clear();         
     }
     
     public String getServiceAddress(){
         return service;
     }
     
     public String getHandle(){
         return handle;
     }

     
     
     //-------------------PROXY-------------------
    /**
	  * sends the body with http-method method to serviceurl. A HTTP-Code will be returned and the
     * Answer-Body from the server will be copied to retbody. In Case of errors (connection problems, bad url ...)
	  * an Exception is thrown. Cookies are stored and used in next call.
     * @param serviceurl the url that should be talked to
     * @param body the body to send, the received body from server
     * @param method HTTP method "PUT", "POST", "GET" ...
     * @return HTTP returncode
     * @throws Exception
     */
    private int httpRequest(String serviceurl, byte[] body, StringBuffer retbody, String method) 
         throws Exception{

		URL url;
		HttpURLConnection con;
		boolean geterrorstream=false;

		try{
			url=new URL (serviceurl);
			con =(HttpURLConnection) url.openConnection();	
            con.setInstanceFollowRedirects(false);
			con.setRequestMethod(method);
            
            for (String key : cookies.keySet()){
                String cookievalue=key+"="+cookies.get(key);
                con.setRequestProperty("Cookie",cookievalue);
            }
            
		}catch (MalformedURLException e){throw new Exception(e.getMessage());}
	 	 catch (ProtocolException e){throw new Exception(e.getMessage());}
		 catch (IOException e){throw new Exception(e.getMessage());}

		prepareTransmission(con, body); 

		try{

			con.connect();
            parseCookies(con);
                       
		}catch (IOException e){
			geterrorstream=true;
		}

		try{
				int responsecode=con.getResponseCode();
                parseCookies(con);                
				if (!geterrorstream && responsecode>=200 && responsecode<300){
					convertInputStream(con.getInputStream(),retbody);
                    
				}else{
					if (con.getErrorStream()!=null)
						convertInputStream(con.getErrorStream(),retbody);					
				}
				return responsecode;
		}catch (IOException e){
				throw new Exception(e.getMessage());
		}
    }

	/**
	* sets the message header and copies the message body into an output stream
	* @param HttpURLConnection con
	* @param byte[] body
	* @throws Exception
	*/
	private void prepareTransmission(HttpURLConnection con,byte[] body) throws Exception{
		try{
			con.setRequestProperty ("Content-Type","application/x-www-form-urlencoded;charset="+DEFAULT_ENCODING.name());

			if (body.length>0){
				con.setDoOutput(true);
				OutputStream out = con.getOutputStream();
				out.write(body);
				out.close();
			}
		}catch (IOException e){throw new Exception(e.getMessage());}

	}

	/**
	* reads the InputStream from the server into a StringBuffer
	* @param InputStream is
	* @param InputStream is, StringBuffer sb
	* @throws Exception
	*/
	private void convertInputStream(InputStream is, StringBuffer sb) throws Exception{
		try{
			BufferedReader reader=new BufferedReader(new InputStreamReader(is, DEFAULT_ENCODING));
			sb.delete(0,sb.length());
			for (String line=reader.readLine(); line !=null ; line=reader.readLine())
				sb.append(line);
			reader.close();
		}catch (IOException e) {throw new Exception(e.getMessage());}
	}
    
    private void parseCookies(HttpURLConnection con){
                        
       int i=0;
       String headervalue;
       String headerkey;
       
       while ((headervalue=con.getHeaderField(i))!=null){

            headerkey = con.getHeaderFieldKey(i);
            i++;
               
            if (headerkey ==null) continue;

            if (headerkey.equals("Set-Cookie")){
            
                int end=headervalue.indexOf(";");
                if (end<0) continue;                
                String set=headervalue.substring(0,end);

                int cut=set.indexOf("=");                
                if (cut<0) continue;
                
                int len=set.length();
            
                String name = set.substring(0,cut);
                String value = set.substring(cut + 1, len);             
                
                cookies.put(name, value);             
            }
        }  
    
    }
    
    
}
