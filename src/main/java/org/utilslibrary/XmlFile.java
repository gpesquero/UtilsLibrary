package org.utilslibrary;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public abstract class XmlFile {
	
	private Document mDocument = null;
	
	public XmlFile() {
	}
	
	abstract protected void onNewElement(String name, HashMap<String, String> tags, String value);
	
	public boolean openFile(String fileName) {
		
		File file = new File(fileName);
		
		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
	
		DocumentBuilder documentBuilder;
		
		try {
			documentBuilder = documentBuilderFactory.newDocumentBuilder();
			
		} catch (ParserConfigurationException e) {
			
			Log.error("newDocumentBuilder() exception: "+e.getMessage());
			Log.error("Quitting...");
			
			return false;
		}
		
		try {
			mDocument = documentBuilder.parse(file);
			
		} catch (SAXException e) {
			
			Log.error("documentBuilder.parse() exception: "+e.getMessage());
			Log.error("Quitting...");
			
			return false;
			
		} catch (IOException e) {
			
			Log.error("documentBuilder.parse() exception: "+e.getMessage());
			Log.error("Quitting...");
			
			return false;
		}
		
		return true;
	}
	
	public boolean processFile() {
		
		if (mDocument == null) {
			
			Log.error("XmlFile.processFile() mDocument == null");
			
			return false;
		}
		
		Element mainElement = mDocument.getDocumentElement();
		
		processElement(mainElement);
		
		return true;
	}
	
	private void processElement(Element element) {
		
		String nodeName = element.getNodeName();
		//String nodeValue = element.getNodeValue();
		String textContent = element.getTextContent();
		
		//Log.info("New Element: name='" + nodeName);
		//Log.info("  Text content: " + textContent);
		
		NamedNodeMap nodeMap = element.getAttributes();
		
		HashMap<String, String> attribs = new HashMap<String, String>();
		
		for (int i=0; i < nodeMap.getLength(); i++) {
			
			Node n = nodeMap.item(i);
			
			String key = n.getNodeName();
			String value = n.getNodeValue();
			
			//Log.info("  Attrib #" + i + ": '" + key + "'='" + value + "'");
			
			attribs.put(key, value);
		}
		
		onNewElement(nodeName, attribs, textContent);
		
		NodeList childNodes = element.getChildNodes();
		
		for(int i=0; i < childNodes.getLength(); i++) {
			
			Node childNode = childNodes.item(i);
			
			/*
			String childNodeName = childNode.getNodeName();
			short childNodeType = childNode.getNodeType();
			
			Log.info("  ChildNode #" + i + ": name='" + childNodeName + "', type=" + childNodeType);
			*/
			
			if (childNode.getNodeType() == Node.ELEMENT_NODE) {
				
				processElement((Element) childNode);
			}
		}
		
		/*
		onMainElementOpen(mainElement);
		
		NodeList childNodes=element.getChildNodes();
		
		for(int i=0; i<childNodes.getLength(); i++) {
			
			Node childNode = childNodes.item(i);
			
			if (childNode.getNodeType() != Node.ELEMENT_NODE)
				continue;
			
			ArrayList<Tag> childTags=null;
			
			if (tags!=null) {
				childTags=new ArrayList<Tag>(tags);
			}
			
			if (!processElement((Element)childNode, childTags)) {
				
				return false;
			}
		}
		*/
	}
}
