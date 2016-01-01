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

import buffer_manager.AbstractBufferManager;
import org.antlr.v4.runtime.misc.Pair;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by airvan21 on 25.10.15.
 */
public class XMLBuilder {

    /*
        Handles Sys.Database XML file:

        1) Uploads it from "pathToSysTable" (if it is already exist)
        2) Creates new Sys.Database XML     (if it doesn't exist)

     */
    public XMLBuilder(String pathToSysTable) {
        pathToXML = pathToSysTable;
        File XMLFile = new File(pathToSysTable);

        if(XMLFile.exists() && !XMLFile.isDirectory()) {
            uploadXMLDocument(XMLFile);
        } else {
            try {
                boolean created = XMLFile.getParentFile().mkdirs();
                if (!created)
                    throw new IOException(String.format("Directory '%s' not created", pathToSysTable));
                created = XMLFile.createNewFile();
                if (!created)
                    throw new IOException(String.format("File '%s' not created", pathToSysTable));
            } catch (IOException e) {
                e.printStackTrace();
            }
            createXMLDocument();
            storeXMLDocument();
        }
    }

    /*
        Check if table with name 'inTableName' already exist
     */
    public Boolean isExist(String inTableName){

        NodeList nList = sysTable.getElementsByTagName("name");
        for (int i = 0; i < nList.getLength(); i++) {
            String tableName = nList.item(i).getTextContent();
            if (tableName.equals(inTableName)) {
                return true;
            }
        }
        return false;
    }

    /*
        Adds description about new table in Sys.Database XML
    */
    public void addRecord(SysInfoRecord sysInfoRecord)
    {
        // "dbms" is a root element
        Element rootElement = sysTable.getDocumentElement();

        // "table" record
        Element sysTableElement = sysTable.createElement("table");
        rootElement.appendChild(sysTableElement);

        // "name"  field in table record
        Element tableName = sysTable.createElement("name");
        tableName.appendChild(sysTable.createTextNode(sysInfoRecord.tableName));
        sysTableElement.appendChild(tableName);

        // "path" field in table record
        Element tablePath = sysTable.createElement("path");
        tablePath.appendChild(sysTable.createTextNode(sysInfoRecord.tablePath));
        sysTableElement.appendChild(tablePath);

        // "indexType" record
        Element indexType = sysTable.createElement("indexType");
        indexType.appendChild(sysTable.createTextNode(sysInfoRecord.indexType));
        sysTableElement.appendChild(indexType);

        // "indexPath" record
        Element indexPath = sysTable.createElement("indexPath");
        indexPath.appendChild(sysTable.createTextNode(sysInfoRecord.indexPath));
        sysTableElement.appendChild(indexPath);
    }

    /*
        Stores Sys.Database XML file on HDD
     */
    public void storeXMLDocument()
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
         Table content reader
    */
    public List<SysInfoRecord> loadContents() {
        List<SysInfoRecord> content = new ArrayList<>();
        NodeList nameList = sysTable.getElementsByTagName("name");
        NodeList pathList = sysTable.getElementsByTagName("path");
        NodeList indexTypeList = sysTable.getElementsByTagName("indexType");
        NodeList indexPathList = sysTable.getElementsByTagName("indexPath");
        String rootdbPath = AbstractBufferManager.DATA_ROOT_DB_FILE.toAbsolutePath().toString();
        for (int i = 0; i < nameList.getLength(); i++) {
            if (!pathList.item(i).getTextContent().equals(rootdbPath))
                content.add(new SysInfoRecord(nameList.item(i).getTextContent(),
                                            pathList.item(i).getTextContent(),
                                            indexTypeList.item(i - 1).getTextContent(),
                                            indexPathList.item(i - 1).getTextContent()));
        }
        return content;
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

            // "dbms" as a root element
            Element rootElement = sysTable.createElement("dbms");
            sysTable.appendChild(rootElement);

            // "table" record
            Element tableElement = sysTable.createElement("table");
            rootElement.appendChild(tableElement);

            // "name"  field in table record
            Element tableName = sysTable.createElement("name");
            tableName.appendChild(sysTable.createTextNode("root_db"));
            tableElement.appendChild(tableName);

            // "path" field in table record
            Path filePath = Paths.get("data//root_db.ndb");
            Element tablePath = sysTable.createElement("path");
            tablePath.appendChild(sysTable.createTextNode(filePath.toAbsolutePath().toString()));
            tableElement.appendChild(tablePath);

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
            sysTable.getDocumentElement().normalize();

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
