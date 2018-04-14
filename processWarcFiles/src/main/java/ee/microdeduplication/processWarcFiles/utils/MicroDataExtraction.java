package ee.microdeduplication.processWarcFiles.utils;

import org.apache.any23.Any23;
import org.apache.any23.extractor.ExtractionException;
import org.apache.any23.source.DocumentSource;
import org.apache.any23.source.StringDocumentSource;
import org.apache.any23.writer.NTriplesWriter;
import org.apache.any23.writer.TripleHandler;
import org.apache.any23.writer.TripleHandlerException;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.ccil.cowan.tagsoup.XMLWriter;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.ccil.cowan.tagsoup.Parser;
import org.ccil.cowan.tagsoup.HTMLSchema;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.xml.sax.*;

/*
 * Created by Khalil Rehman and Madis-Karli Koppel
 * Extracts ntriples from strings (Html and xml files)
 */
public class MicroDataExtraction {

    private static final Logger logger = LogManager.getLogger(MicroDataExtraction.class);

    List<String> statementsList;

    /*
     * Actual workhorse of this application. This is the part that extracts ntriples
     * @param string contents - string containing html or xml data, not verified
     * @return string ntriples - can return several, each ntriple on new line
     */
    public String extractMicroData(String key, String contents) {

        logger.debug("In extracting microdata.");
        // When setting the extractor to html-microdata we only get microdata such as schema.org etc
        // when not defining it then we do get a lot of noise
        // So we need to find correct extractors.
        // 1.1 available:
        // csv, html-head-icbm, html-head-links, html-head-title, html-mf-adr, html-mf-geo,
        // html-mf-hcalendar, html-mf-hcard, html-mf-hlisting, html-mf-hrecipe, html-mf-hresume, html-mf-hreview,
        // html-mf-hreview-aggregate, html-mf-license, html-mf-species, html-mf-xfn, html-microdata, html-rdfa11,
        // html-script-turtle, html-xpath, rdf-jsonld, rdf-nq, rdf-nt, rdf-trix, rdf-turtle, rdf-xml
        Any23 runner = new Any23("html-rdfa11");
        //Any23 runner = new Any23();

        // to fix java.nio.charset.UnsupportedCharsetException: Charset IBM424_rtl is not supported
        // doesn't actually work
        //contents = Charset.forName("UTF-8").encode(contents).toString();

        contents = fixHtml(contents);

        // uri needs be <string>:/<string> or it will throw ExtractionException"Error in base IRI:"
        DocumentSource source = new StringDocumentSource(contents, "java:/MicroDataExtraction");

        // handler writes ntriples into bytearrayoutputstream
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TripleHandler handler = new NTriplesWriter(out);

        logger.trace("Triple handler occupied.");
        String result = "";

        // clean the html data, html-rdfa11 from tpilet.ee
        // before
        // all
        // 2.2  2121    1485 terms, 457 elements
        // after (no header fix)
        // 2.2  2105    1730 terms, 196 elements
        // header fix and html fix
        // 22   1853    1479 terms
        // only header fix
        // 2.2  2279    1561 terms, 457 elements
        // correct printing
        // html and doctype fix
        // "<java:/MicroDataExtraction> <dc" 263
        // purl.org 1853
        // doctype fix
        // purl.org 2279
        // "<java:/MicroDataExtraction> <dc" 0


        try {
            logger.trace("Extracting microdata.");

            runner.extract(source, handler);
        } catch (ExtractionException e) {
            // org.apache.any23.extractor.ExtractionException: Error while processing on subject '_:node1a83b68da17a5c2fd93c11217f74d4c' the itemProp: '{ "xpath" : "/HTML[1]/BODY[1]/DIV[3]/FORM[1]/INPUT[1]", "name" : "query-input", "value" : { "content" : "Null", "type" : "Link" } }'
            logger.error(e.getMessage());
            logger.error("ExtractionException " + key);
        } catch (java.nio.charset.UnsupportedCharsetException e) {
            // Charset IBM424_rtl is not supported
            // at org.apache.any23.extractor.html.TagSoupParser.<init>(TagSoupParser.java:83)
            logger.debug(e.getMessage());
            logger.error("UnsupportedCharsetException " + key);
            e.printStackTrace();
        } catch (RuntimeException e) {
            // Error while retrieving mime type.
            // at org.apache.any23.mime.TikaMIMETypeDetector.guessMIMEType(TikaMIMETypeDetector.java:271)
            // Caused by: java.io.IOException: (line 0) invalid char between encapsulated token end delimiter
            // at org.apache.commons.csv.CSVParser.encapsulatedTokenLexer(CSVParser.java:510)
            result = out.toString();
            if (!result.isEmpty()) {
//                logger.error("got " + out.toString());
            }
            logger.debug(e.getMessage());
//            logger.error("RuntimeException " + key);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Unknown exception " + key);
            logger.error(e.getMessage());
        } catch (OutOfMemoryError e) {
            logger.error("OutOfMemoryError " + key);
            logger.debug(e.getMessage());
        } catch (StackOverflowError e) {
            logger.error("StackOverflowError " + key);
        } catch (Error e) {
            logger.error("Unknown Error : " + e.getMessage() + key);
        } finally {
            try {
                out.close();
                handler.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (TripleHandlerException e) {
                e.printStackTrace();
            }
        }

        // This needs to be here, not in try, otherwise some triples are lost
        // store any23 results in result String, ntriples are separated by newline
        result = out.toString();

        if (!result.isEmpty()) {
            result = removeDuplicateTriples(result);

//            if (result.contains("purl.org") || result.contains("<java:/MicroDataExtraction> <dc:")) {
//                String[] parts = result.split("\n");
//                for (String p : parts) {
//                    if (p.contains("<http://purl.org") || p.contains("<java:/MicroDataExtraction> <dc:")) {
//                        System.out.println(p);
//                    }
//                }
//            }

            // This is a common occurence, but only happens with real data
            if (!result.substring(result.length() - 2, result.length() - 1).equals(".")) {
                logger.error("BROKEN RESULT1!");
                logger.error(key);
                logger.error(result);
                logger.error(result.substring(result.length() - 1, result.length()));
            }
            // TODO remove duplicates
            return result;
        } else {
            return "";
        }
    }

    /*
     * @param string nTriples - nTriples, separated by newline
     * @return string nTriples - nTriples without duplicates, separated by newline
     */
    private String removeDuplicateTriples(String nTriples) {
        String[] tripleStatements = nTriples.split("\\\n");
        Set<String> triplesSet = new LinkedHashSet<String>(Arrays.asList(tripleStatements));
        String uResult = "";
        for (String statement : triplesSet) {
            uResult += statement + "\n";
        }
        logger.debug(uResult);
        return uResult;
    }

    /*
     * Convert ntriples into n-quads
     * n-quads are stored in field statementsList
     * @param string key - the key from warc file containing triple location and date of extraction
     * @param string nTriples - ntriples, several, separated by new line
     */
    public void setStatements(String key, String nTriples) {

        String[] statements = nTriples.split("(\\s\\.)(\\r?\\n)");
        StringBuilder stat;

        for (String statement : statements) {

            if (statement.length() == 0) {
                continue;
            }

            stat = new StringBuilder("");

            String[] statParts = statement.split("\\s(<|\"|_)");

            try {
                String check = statParts[0] + statParts[1] + statParts[2];
            } catch (ArrayIndexOutOfBoundsException e) {
                continue;
            }

            String subject = statParts[0].replaceAll("(<|>|\")", "");
            String predicate = statParts[1].replaceAll("(<|>|\")", "");
            String object = statParts[2].replaceAll("(<|>|\")", "");

            stat.append("<" + key + ">, ").append("<" + subject + ">, ")
                    .append("<" + predicate + ">, ").append("<" + object + ">");

            statementsList.add(stat.toString());

            logger.debug(statement);
            logger.debug(stat.toString());
        }

        if (false) {
//      When triples were parsed by splitting then
//      Empty strings were also split that caused exceptions
//      Now strings are not split but we still do not want to work with empty nTriples
//        if (nTriples.length() == 0)
//            throw new ArrayIndexOutOfBoundsException();

//        String[] statements = nTriples.split("(\\s\\.)(\\r?\\n)"); // old way
//            // TODO guard if the split is messed up and produces a list too long
//            String[] statements = nTriples.split("(\\s\\.)(\\r?\\n)");
//
//            for (String statement : statements) {
//
//
//                if (statement.length() == 0)
//                    continue;
//
//                String subject = null;
//                String predicate = null;
//                String object = null;
//
//                try {
//                    Model model = Rio.parse(new StringReader(statement + " ."), key, RDFFormat.NTRIPLES);
//
//                    RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);
//                    ParserConfig parserConfig = new ParserConfig();
//                    parserConfig.isPreserveBNodeIDs();
//                    rdfParser.setParserConfig(parserConfig);
//
//                    subject = removeGeneratedID(model.subjects().toArray()[0].toString());
//                    predicate = model.predicates().toArray()[0].toString();
//                    object = removeGeneratedID(model.objects().toArray()[0].toString());
//
////                logger.error(subject);
////                logger.error(predicate);
////                logger.error(object);
//
//                } catch (IOException e) {
//                    logger.error("IOException when parsing " + key);
//                    e.printStackTrace();
//                } catch (RDFParseException e) {
//                    logger.error("RDFParseException when parsing " + key);
//                    e.printStackTrace();
//                } catch (ArrayIndexOutOfBoundsException e) {
//                    logger.error("ArrayIndexOutOfBoundsException when parsing " + key);
//                } catch (Exception e) {
//                    logger.error("Exception when parsing " + key);
//                    e.printStackTrace();
//                }
//
//                //TODO think this through - it is not the best to add it here as right now we add it to EVERY SINGLE NODE IN THE GRAPH
//                // Create a mined-from key so that we can combine triples from one site
//                statementsList.add(subject + " <IR/mined-from>" + " \"" + key + "\" .");
//                // add the ntriple to output
//                statementsList.add(statement + " .");
//            }
        }
    }

    /*
     * removes genid-.. from node that is added there by Rio.parse
     * if input does not match pattern then input is returned
     */
    private String removeGeneratedID(String input) {

        // check if input string matches genid pattern
        Pattern testPattern = Pattern.compile("_:genid-.*-node.*");
        Matcher m = testPattern.matcher(input);
        if (!m.find())
            return input;

        // extract the node part from the string
        Pattern realPattern = Pattern.compile("node.*");
        m = realPattern.matcher(input);
        if (m.find()) {
            return "_:" + m.group(0);
        }

        return input;
    }


    private String fixHtml2(String contents) {
        Document doc = Jsoup.parse(contents);
        return fixDoctype(doc.html());
    }

    private String fixHtml(String contents) {
        InputStream stream = new ByteArrayInputStream(contents.getBytes());

        HTMLSchema schema = new HTMLSchema();
        XMLReader reader = new Parser();

        try {
            reader.setProperty(Parser.schemaProperty, schema);
        } catch (SAXNotRecognizedException | SAXNotSupportedException e) {
            e.printStackTrace();
        }

        ByteArrayOutputStream out1 = new ByteArrayOutputStream(contents.getBytes().length + 100);
        Writer writeger = new OutputStreamWriter(out1);
        XMLWriter x = new XMLWriter(writeger);

        reader.setContentHandler(x);

        InputSource s = new InputSource(stream);
        String contents0 = "";
        try {
            reader.parse(s);
            contents0 = IOUtils.toString(new ByteArrayInputStream(out1.toByteArray()));
            contents = insertDoctypeFromSource(contents0, contents);
        } catch (Exception e) {
            return contents;
        }

        return contents;
    }

    private String fixDoctype(String sourceHTML) {
        String oldDoctype = "";
        String fixedDoctype = "";
        Pattern doctypePattern = Pattern.compile("<!DOCTYPE[^>]*>");
        Matcher doctypeMatcher = doctypePattern.matcher(sourceHTML);

        if (doctypeMatcher.find())
            oldDoctype = doctypeMatcher.group(0);

        if (oldDoctype.length() < 1)
            return sourceHTML;

        // White spaces are required between publicId and systemId.
        // <!DOCTYPE HTML PUBLIC ""> becomes <!DOCTYPE HTML PUBLIC "" "">
        String[] check = oldDoctype.replaceAll("\n", "").split("\"");

        // chekc 1 is ok
        if (check.length == 3) {
            // maybe not set the strict stuff
            fixedDoctype = oldDoctype.substring(0, oldDoctype.length() - 1) + "  \"http://www.w3.org/TR/html4/strict.dtd\">";
        }

        return sourceHTML.replaceFirst(Pattern.quote(oldDoctype), fixedDoctype);
    }

    protected static String insertDoctypeFromSource(String toBeReplacedHTML, String sourceHTML) {
        String oldDoctype = "";
        String currentHTMLHeader = "";
        String xmlHeader = "<?xml version=\"1.0\" standalone=\"yes\"?>";

        Pattern doctypePattern = Pattern.compile("<!DOCTYPE[^>]*>");
        Matcher doctypeMatcher = doctypePattern.matcher(sourceHTML);

        if (doctypeMatcher.find())
            oldDoctype = doctypeMatcher.group(0);

        if (oldDoctype.length() < 1)
            return toBeReplacedHTML;

        // White spaces are required between publicId and systemId.
        // <!DOCTYPE HTML PUBLIC ""> becomes <!DOCTYPE HTML PUBLIC "" "">
        String[] check = oldDoctype.replaceAll("\n", "").split("\"");

        // chekc 1 is ok
        if (check.length == 3) {
            // maybe not set the strict stuff
            oldDoctype = oldDoctype.substring(0, oldDoctype.length() - 1) + "  \"http://www.w3.org/\">";
        }

        Pattern htmlPattern = Pattern.compile("<html[^>]*>");
        Matcher currentHeaderMatcher = htmlPattern.matcher(toBeReplacedHTML);
        if (currentHeaderMatcher.find())
            currentHTMLHeader = currentHeaderMatcher.group(0);

        // <html> is length 6
        if (currentHTMLHeader.length() < 6) {
            toBeReplacedHTML = toBeReplacedHTML.replaceFirst(Pattern.quote(xmlHeader), xmlHeader + "\n" + oldDoctype);
            return toBeReplacedHTML;
        }

        return toBeReplacedHTML.replaceFirst(Pattern.quote(currentHTMLHeader), oldDoctype + "\n" + currentHTMLHeader);
    }

    public MicroDataExtraction(List<String> statementsList) {
        this.statementsList = statementsList;
    }

    public List<String> getStatements() {
        return statementsList;
    }

}
