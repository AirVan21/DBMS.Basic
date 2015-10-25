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

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by airvan21 on 25.10.15.
 */
public class XMLBuilder {

    public XMLBuilder(String path) {
        this.pathToXML = path;
        createXMLDocument();
        /*
        File XMLFile = new File(path);
        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

            Document doc = dBuilder.parse(XMLFile);
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            System.out.println("XML Table wasn't found");
            e.printStackTrace();
        }*/
    }

    private void createXMLDocument()
    {
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

            Document doc = docBuilder.newDocument();

            // "table" record as a root element
            Element rootElement = doc.createElement("table");
            doc.appendChild(rootElement);

            // "name"  field in table record
            Element tableName = doc.createElement("name");
            tableName.appendChild(doc.createTextNode("root_db"));
            rootElement.appendChild(tableName);

            // "path" field in table record
            Path filePath = Paths.get("data//root_db.ndb");
            Element tablePath = doc.createElement("path");
            tablePath.appendChild(doc.createTextNode(filePath.toAbsolutePath().toString()));
            rootElement.appendChild(tablePath);

            // Write created XML
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(doc);

            //StreamResult result = new StreamResult(new File(pathToXML));

            // Output to console for testing
            StreamResult result = new StreamResult(System.out);

            transformer.transform(source, result);

        } catch (ParserConfigurationException e) {
            System.out.println("createXMLDocument()");
            e.printStackTrace();
        } catch (TransformerConfigurationException e) {
            System.out.println("createXMLDocument()");
            e.printStackTrace();
        } catch (TransformerException e) {
            System.out.println("createXMLDocument()");
            e.printStackTrace();
        }
    }

    private String pathToXML;

}
