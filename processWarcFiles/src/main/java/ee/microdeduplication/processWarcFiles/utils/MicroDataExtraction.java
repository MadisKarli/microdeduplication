package ee.microdeduplication.processWarcFiles.utils;

import org.apache.any23.Any23;
import org.apache.any23.extractor.ExtractionException;
import org.apache.any23.source.DocumentSource;
import org.apache.any23.source.StringDocumentSource;
import org.apache.any23.writer.NTriplesWriter;
import org.apache.any23.writer.TripleHandler;
import org.apache.any23.writer.TripleHandlerException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.ParseErrorLogger;
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

        // to fix java.nio.charset.UnsupportedCharsetException: Charset IBM424_rtl is not supported
        // doesn't actually work
        //contents = Charset.forName("UTF-8").encode(contents).toString();

//        contents = fixHtml(contents);


        // 2.2
        String[] extractors = new String[]{
                "csv",
                "html-embedded-jsonld",
                "html-head-icbm",
                "html-head-links",
                "html-head-meta",
                "html-head-title",
                "html-mf-adr",
                "html-mf-geo",
                "html-mf-hcalendar",
                "html-mf-hcard",
                "html-mf-hlisting",
                "html-mf-hrecipe",
                "html-mf-hresume",
                "html-mf-hreview",
                "html-mf-hreview-aggregate",
                "html-mf-license",
                "html-mf-species",
                "html-mf-xfn",
                "html-microdata",
                "html-rdfa11",
                "html-xpath",
                "owl-functional",
                "owl-manchester",
                "rdf-jsonld",
                "rdf-nq",
                "rdf-nt",
                "rdf-trix",
                "rdf-turtle",
                "rdf-xml",
                "yaml"};

        extractors = new String[]{"html-microdata"};

        // 1.1
//        extractors = new String[]{"csv", "html-head-icbm", "html-head-links", "html-head-title", "html-mf-adr", "html-mf-geo",
//                "html-mf-hcalendar", "html-mf-hcard", "html-mf-hlisting", "html-mf-hrecipe", "html-mf-hresume", "html-mf-hreview",
//                "html-mf-hreview-aggregate", "html-mf-license", "html-mf-species", "html-mf-xfn", "html-microdata", "html-rdfa11",
//                "html-script-turtle", "html-xpath", "rdf-jsonld", "rdf-nq", "rdf-nt", "rdf-trix", "rdf-turtle", "rdf-xml"
//        };

        String combinedResult = "";

        for (String extractor : extractors) {
            String stuff = useExtractor(extractor, contents, key);
            combinedResult += stuff;
        }
        return combinedResult;
    }


    private String useExtractor(String extractorName, String contents, String key) {
        Any23 runner = new Any23(extractorName);

        String[] keyParts = key.split("::");
        // uri needs be <string>:/<string> or it will throw ExtractionException"Error in base IRI:"
        // DocumentSource source = new StringDocumentSource(contents, "java:/MicroDataExtraction");
        DocumentSource source = new StringDocumentSource(contents, keyParts[0] + "://" + keyParts[1] + keyParts[2]);
        // using the whole key causes exceptions
        // DocumentSource source = new StringDocumentSource(contents, key);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TripleHandler handler = new NTriplesWriter(out);

        logger.trace("Triple handler occupied.");
        String result = "";

        try {
            logger.trace("Extracting microdata.");

            runner.extract(source, handler);
        } catch (ExtractionException e) {
            // org.apache.any23.extractor.ExtractionException: Error while processing on subject '_:node1a83b68da17a5c2fd93c11217f74d4c' the itemProp: '{ "xpath" : "/HTML[1]/BODY[1]/DIV[3]/FORM[1]/INPUT[1]", "name" : "query-input", "value" : { "content" : "Null", "type" : "Link" } }'
            logger.debug(e.getMessage());
            logger.error("ExtractionException " + key + " " + extractorName);
        } catch (java.nio.charset.UnsupportedCharsetException e) {
            // Charset IBM424_rtl is not supported
            // at org.apache.any23.extractor.html.TagSoupParser.<init>(TagSoupParser.java:83)
            logger.debug(e.getMessage());
            logger.error("UnsupportedCharsetException " + key + " " + extractorName);
        } catch (RuntimeException e) {
            // Error while retrieving mime type.
            // at org.apache.any23.mime.TikaMIMETypeDetector.guessMIMEType(TikaMIMETypeDetector.java:271)
            // Caused by: java.io.IOException: (line 0) invalid char between encapsulated token end delimiter
            // at org.apache.commons.csv.CSVParser.encapsulatedTokenLexer(CSVParser.java:510)
            logger.debug(e.getMessage());
            logger.error("RuntimeException " + key + " " + extractorName);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Unknown exception " + key + " " + extractorName);
            logger.error(e.getMessage());
        } catch (OutOfMemoryError e) {
            logger.error("OutOfMemoryError " + key + " " + extractorName);
            logger.debug(e.getMessage());
        } catch (StackOverflowError e) {
            logger.error("StackOverflowError " + key + " " + extractorName);
        } catch (Error e) {
            logger.error("Unknown Error : " + e.getMessage() + " " + key + " " + extractorName);
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

            // This is a common occurence, but only happens with real data
            if (!result.substring(result.length() - 2, result.length() - 1).equals(".")) {
                logger.error("BROKEN RESULT1!");
                logger.error(key);
                logger.error(result);
                logger.error(result.substring(result.length() - 1, result.length()));
            }
            // TODO remove duplicates
            return result;
        }
        return "";
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

        Set<String> subjects = new HashSet<String>();

        for (String statement : statements) {

            if (statement.length() == 0)
                continue;

            statement = statement + " .";
//            logger.error(statement);

            String subject = null;
            String predicate = null;
            String object = null;


            // This here parses the extracted triples to make sure they are all correct ones
            // breaks when a triple is not according to standard, then nothing is returned
            try {
                OutputStream outputStream = new ByteArrayOutputStream();

                RDFWriter rdfWriter = Rio.createWriter(RDFFormat.NTRIPLES, outputStream);

                RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);

                ParserConfig parserConfig = new ParserConfig();
                parserConfig.set(BasicParserSettings.PRESERVE_BNODE_IDS, true);
                // IRI includes string escapes: '\34'
                // https://openrdf.atlassian.net/browse/SES-2390
                parserConfig.set(BasicParserSettings.VERIFY_URI_SYNTAX, false);

                rdfParser.setParserConfig(parserConfig);
                rdfParser.setRDFHandler(rdfWriter);

                rdfParser.parse(new StringReader(statement), "");

                statementsList.add(outputStream.toString().replaceAll("\n", ""));
            } catch (IOException e) {
                logger.error("IOException when parsing " + key);
                e.printStackTrace();
            } catch (RDFParseException e) {
                logger.error("RDFParseException when parsing " + key);
//                e.printStackTrace();
            } catch (Exception e) {
                logger.error("Exception when parsing " + key);
                e.printStackTrace();
            }
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


    public MicroDataExtraction(List<String> statementsList) {
        this.statementsList = statementsList;
    }

    public List<String> getStatements() {
        return statementsList;
    }

}
