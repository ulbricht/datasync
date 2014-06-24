package de.gfz_potsdam.datasync;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.File;

import java.util.HashMap;

import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.Controller;



public class XSLTTransformer {
	


	public static byte [] convert(String xsltPath, byte[] orig, HashMap<String,String> parameters) throws IOException, TransformerException {

		Source xsltSource;
		xsltSource = new StreamSource(new File(xsltPath));
		TransformerFactory tFactory = TransformerFactory.newInstance();

		XSLTErrorListener conversionmessages=new XSLTErrorListener();
		XSLTErrorListener factorymessages=new XSLTErrorListener();


		tFactory.setErrorListener(factorymessages);

		Transformer transformer = tFactory.newTransformer(xsltSource);

		((Controller)transformer).setMessageEmitter(new XSLTMessageWarner());//read xsl:message - output

		transformer.setErrorListener(conversionmessages); 

        for (String parametername : parameters.keySet()){
        	transformer.setParameter(parametername, parameters.get(parametername));
        }
        
		ByteArrayInputStream input = new ByteArrayInputStream(orig);
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try{
			transformer.transform(new StreamSource(input),new StreamResult(output));
		}catch (TransformerException e){
			//exception is thrown when it should not
		}

		if (factorymessages.isError())
			throw new TransformerException(factorymessages.toString());
		if (conversionmessages.isError())
			throw new TransformerException(conversionmessages.toString());
        
		return output.toByteArray(); 


	}

}
