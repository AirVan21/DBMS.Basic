package common.xml;


import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by airvan21 on 25.10.15.
 */
public class XMLBuilder {

    /*
        Handles Sys.Database XML file:

        1) Uploads it from "pathToSysTable" (if it is already exist)
        2) Creates new Sys.Database XML (if it doesn't exist)
     */
    public XMLBuilder(String pathToSysTable) {
        pathToXML = pathToSysTable;
        File XMLFile = new File(pathToSysTable);

        if(XMLFile.exists() && !XMLFile.isDirectory()) {
            uploadXMLDocument(XMLFile);
        } else {
            createXMLDocument();
            storeXMLDocument();
        }
    }

    /*
        Stores Sys.Database XML file on HDD
     */
    private void storeXMLDocument()
    {
        try {

            // Prepare created XML
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(sysTable);

            // Write result to File
            StreamResult result = new StreamResult(new File(pathToXML));
            transformer.transform(source, result);

        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        } catch (TransformerException e) {
            e.printStackTrace();
        }
    }

    /*
        Creates Sys.Database XML file from scratch
     */
    private void createXMLDocument()
    {
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

            sysTable = docBuilder.newDocument();

            // "table" record as a root element
            Element rootElement = sysTable.createElement("table");
            sysTable.appendChild(rootElement);

            // "name"  field in table record
            Element tableName = sysTable.createElement("name");
            tableName.appendChild(sysTable.createTextNode("root_db"));
            rootElement.appendChild(tableName);

            // "path" field in table record
            Path filePath = Paths.get("data//root_db.ndb");
            Element tablePath = sysTable.createElement("path");
            tablePath.appendChild(sysTable.createTextNode(filePath.toAbsolutePath().toString()));
            rootElement.appendChild(tablePath);

        } catch (ParserConfigurationException e) {
            System.out.println("createXMLDocument()");
            e.printStackTrace();
        }
    }

    /*
        Uploads Sys.Database XML file from HDD
     */
    private void uploadXMLDocument(File XMLFile)
    {
        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            // Uploads existing Sys.Database XML
            sysTable = dBuilder.parse(XMLFile);

        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private String   pathToXML;
    private Document sysTable;

}
