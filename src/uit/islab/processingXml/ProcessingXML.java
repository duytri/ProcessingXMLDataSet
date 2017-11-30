package uit.islab.processingXml;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.bson.Document;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class ProcessingXML {

	public static void main(String[] args) {
		System.out.println("Input directory: " + args[0]);

		// Mongo config
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		MongoDatabase database = mongoClient.getDatabase("allvnexpress");
		MongoCollection<Document> collAll = database.getCollection("all");
		MongoCollection<Document> collCrawledLinks = database
				.getCollection("crawledLinks");
		
		Logger mongoLogger = Logger.getLogger( "org.mongodb" );
		mongoLogger.setLevel(Level.SEVERE); // e.g. or Log.WARNING, etc.

		try {
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();

			DefaultHandler handler = new DefaultHandler() {

				int iElemCount = 0;
				long startTime, endTime;
				Document dDoc, dLink;
				boolean bContent = false;
				boolean bLink = false;
				boolean bSubject = false;
				boolean bTitle = false;

				public void startDocument() throws SAXException {
					System.out.println("Start processing...");
					startTime = System.currentTimeMillis();
				}

				public void endDocument() throws SAXException {
					endTime = System.currentTimeMillis();
					float duration = (float) (endTime - startTime) / 1000;
					System.out.println("Completed in " + duration + " (s).");
				}

				public void startElement(String uri, String localName,
						String qName, Attributes attributes)
						throws SAXException {
					// System.out.println("Start Element :" + qName);
					if (qName.equalsIgnoreCase("ITEM")) {
						dDoc = new Document();
						dLink = new Document();
					}
					if (qName.equalsIgnoreCase("CONTENT")) {
						bContent = true;
					}

					if (qName.equalsIgnoreCase("LINK")) {
						bLink = true;
					}

					if (qName.equalsIgnoreCase("SUBJECT")) {
						bSubject = true;
					}

					if (qName.equalsIgnoreCase("TITLE")) {
						bTitle = true;
					}
				}

				public void endElement(String uri, String localName,
						String qName) throws SAXException {
					if (qName.equalsIgnoreCase("ITEM")) {
						iElemCount++;
						collAll.insertOne(dDoc);
						collCrawledLinks.insertOne(dLink);
						System.out.println("Completed Element :" + iElemCount);
					}
				}

				public void characters(char ch[], int start, int length)
						throws SAXException {
					System.out.println(new String(ch, start, length));
					if (bContent) {
						dDoc.append("content", new String(ch, start, length));
						/*
						 * System.out.println("First Name : " + new String(ch,
						 * start, length));
						 */
						bContent = false;
					}
					if (bLink) {
						dDoc.append("link", new String(ch, start, length));
						dLink.append("crawled", new String(ch, start, length));
						/*
						 * System.out.println("Last Name : " + new String(ch,
						 * start, length));
						 */
						bLink = false;
					}
					if (bSubject) {
						dDoc.append("subject", new String(ch, start, length));
						/*
						 * System.out.println("Nick Name : " + new String(ch,
						 * start, length));
						 */
						bSubject = false;
					}
					if (bTitle) {
						dDoc.append("title", new String(ch, start, length));
						/*
						 * System.out.println("Salary : " + new String(ch,
						 * start, length));
						 */
						bTitle = false;
					}
				}
			};

			File file = new File(args[0]);
			InputStream inputStream = new FileInputStream(file);
			Reader reader = new InputStreamReader(inputStream, "UTF-8");

			InputSource is = new InputSource(reader);
			is.setEncoding("UTF-8");

			saxParser.parse(is, handler);

			mongoClient.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
