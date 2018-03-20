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
import org.json4s.Extraction;
import org.openrdf.model.Model;
import org.openrdf.rio.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

        Any23 runner = new Any23("html-microdata");

        // to fix java.nio.charset.UnsupportedCharsetException: Charset IBM424_rtl is not supported
//        contents = Charset.forName("UTF-8").encode(contents).toString();

        // uri needs be <string>:<string> or it will not work
        DocumentSource source = new StringDocumentSource(contents, "java:MicroDataExtraction");

        // handler writes ntriples into bytearrayoutputstream
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TripleHandler handler = new NTriplesWriter(out);


        logger.debug("Triple handler occupied.");
        String result = "";

        try {
            logger.debug("Extracting microdata.");
            runner.extract(source, handler);
//            throw new ExtractionException("s");
        } catch (ExtractionException e) {
            // org.apache.any23.extractor.ExtractionException: Error while processing on subject '_:node1a83b68da17a5c2fd93c11217f74d4c' the itemProp: '{ "xpath" : "/HTML[1]/BODY[1]/DIV[3]/FORM[1]/INPUT[1]", "name" : "query-input", "value" : { "content" : "Null", "type" : "Link" } }'
            logger.debug(e.getMessage());
            logger.debug("ExtractionException " + key);
            return "EXCEPTION:ExtractionException";
        } catch (java.nio.charset.UnsupportedCharsetException e) {
            // Charset IBM424_rtl is not supported
            // at org.apache.any23.extractor.html.TagSoupParser.<init>(TagSoupParser.java:83)
            logger.debug(e.getMessage());
            logger.debug("UnsupportedCharsetException " + key);
            return "EXCEPTION:UnsupportedCharsetException";
        } catch (RuntimeException e) {
            // TODO change to logger.debug
            // Error while retrieving mime type.
            // at org.apache.any23.mime.TikaMIMETypeDetector.guessMIMEType(TikaMIMETypeDetector.java:271)
            // Caused by: java.io.IOException: (line 0) invalid char between encapsulated token end delimiter
            // at org.apache.commons.csv.CSVParser.encapsulatedTokenLexer(CSVParser.java:510)
            logger.debug(e.getMessage());
            logger.debug("Unknown exception " + key);
            return "EXCEPTION:RuntimeException";
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Unknown exception " + key);
            logger.error(e.getMessage());
            return "EXCEPTION:Exception";
        } catch (OutOfMemoryError e) {
            logger.error("OutOfMemoryError " + key);
            logger.error(e.getMessage());
            return "EXCEPTION:OutOfMemoryError";
        } catch (StackOverflowError e) {
            logger.error("StackOverflowError " + key);
            return "EXCEPTION:StackOverflowError";
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
            // This is a common occurence, but only happens with real data
            if (result.substring(result.length() - 1, result.length()).equals("<")) {
                logger.error("BROKEN RESULT!");
                logger.error(key);
            }
            return removeDuplicateTriples(result);
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

            try{
                String check = statParts[0] + statParts[1] + statParts[2];
            } catch (ArrayIndexOutOfBoundsException e){
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

    public MicroDataExtraction(List<String> statementsList) {
        this.statementsList = statementsList;
    }

    public List<String> getStatements() {
        return statementsList;
    }

}
