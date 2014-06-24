package de.gfz_potsdam.datasync;

import javax.xml.transform.ErrorListener;
import javax.xml.transform.TransformerException;


public class XSLTErrorListener implements ErrorListener {
	

	StringBuffer message;

	public XSLTErrorListener(){
		message=new StringBuffer();
	}

	@Override
 	public void error(TransformerException exception) throws TransformerException {
		message.append(exception.getMessage());
	}
	@Override
 	public void fatalError(TransformerException exception) throws TransformerException{
		message.append(exception.getMessage());
	}
	@Override
 	public void warning(TransformerException exception) throws TransformerException{
		message.append(exception.getMessage());
	}
	
	public boolean isError(){
		return (message.length()!=0);
	}

	public String toString(){
		return message.toString();
	}
}
