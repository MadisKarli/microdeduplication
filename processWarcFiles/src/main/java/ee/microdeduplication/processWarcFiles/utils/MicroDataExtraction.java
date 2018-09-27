package ee.microdeduplication.processWarcFiles.utils;

import org.apache.any23.Any23;
import org.apache.any23.extractor.ExtractionException;
import org.apache.any23.extractor.ExtractionParameters;
import org.apache.any23.source.DocumentSource;
import org.apache.any23.source.StringDocumentSource;
import org.apache.any23.writer.NTriplesWriter;
import org.apache.any23.writer.TripleHandler;
import org.apache.any23.writer.TripleHandlerException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tika.io.IOUtils;
import org.ccil.cowan.tagsoup.HTMLSchema;
import org.ccil.cowan.tagsoup.Parser;
import org.ccil.cowan.tagsoup.XMLWriter;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.xml.sax.InputSource;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;

import java.io.*;
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

    private static final boolean FIX_TRIPLES = false;

    private static final boolean CREATE_QUADS = true;

    private static final boolean WORKAROUND_VCARD_INSERT_DOMAIN = true;

    private static final boolean WORKAROUND_BROKEN_OGP = true;

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

        contents = fixHtml(contents);

        if (WORKAROUND_BROKEN_OGP) {
            contents = contents.replaceAll("property=\"article:", "property=\"og:article:");
            contents = contents.replaceAll("property=\"profile:", "property=\"og:profile:");
        }

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

//        extractors = new String[]{"html-rdfa11"};

        String combinedResult = "";

        //logger.error("parsing " + key);
        for (String extractor : extractors) {
            combinedResult += useExtractor(extractor, contents, key);
        }

        return combinedResult;
    }


    private String useExtractor(String extractorName, String contents, String key) {
        Any23 runner = new Any23(extractorName);

        ExtractionParameters extractionParameters = ExtractionParameters.newDefault();

        String[] keyParts = key.split("::");

        // uri needs be <string>:/<string> or it will throw ExtractionException"Error in base IRI:"
        // using the whole key causes exceptions
        DocumentSource source = new StringDocumentSource(contents, keyParts[0] + "://" + keyParts[1] + keyParts[2]);


        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TripleHandler handler = new NTriplesWriter(out);

        String result = "";

        try {
            logger.trace("Extracting microdata.");

            runner.extract(extractionParameters, source, handler);
        } catch (ExtractionException e) {
            // org.apache.any23.extractor.ExtractionException: Error while processing on subject '_:node1a83b68da17a5c2fd93c11217f74d4c' the itemProp: '{ "xpath" : "/HTML[1]/BODY[1]/DIV[3]/FORM[1]/INPUT[1]", "name" : "query-input", "value" : { "content" : "Null", "type" : "Link" } }'
            logger.debug(e.getMessage());
            logger.error("ExtractionException " + key + " " + extractorName);
        } catch (java.nio.charset.UnsupportedCharsetException e) {
            // Charset IBM424_rtl is not supported
            // at org.apache.any23.extractor.html.TagSoupParser.<init>(TagSoupParser.java:83)
            logger.debug(e.getMessage());
            logger.error("UnsupportedCharsetException " + key + " " + extractorName);
        } catch (UnsupportedRDFormatException e) {
            logger.error("UnsupportedRDFormatException " + key + " " + extractorName);
            //e.printStackTrace();
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
        } catch (NoSuchMethodError e) {
            logger.error("NoSuchMethodError : " + e.getMessage() + " " + key + " " + extractorName);
            // html-embedded-jsonld throws this quite often
        } catch (Error e) {
            logger.error("Unknown Error : " + e.getMessage() + " " + key + " " + extractorName);
            e.printStackTrace();
            // org.apache.commons.compress.archivers.ArchiveStreamFactory;
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

            // This is a common occurrence, but only happens with real data
            if (!result.substring(result.length() - 2, result.length() - 1).equals(".")) {
                logger.error("BROKEN RESULT1!");
                logger.error(key);
                logger.error(result);
                logger.error(result.substring(result.length() - 1, result.length()));
            }
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

        for (String statement : statements) {

            if (statement.length() == 0)
                continue;

            statement = statement + " .";

            // Vcard, stupid, does not contain a reference to domain
            // as a workaround, take domain from key and insert it there
            if (WORKAROUND_VCARD_INSERT_DOMAIN) {
                statement = addURLToTriple(key, statement);
            }

            if (FIX_TRIPLES) {
                statement = fixTriple(key, statement);
            }


            if (CREATE_QUADS) {
                // To get quads we could also use the quads writer but it will not give date information
                statement = "<" + key + "> " + statement;
            }

            statementsList.add(statement);
            //logger.error(statementsList);
        }
    }

    private String addURLToTriple(String key, String statement){
        // only work with vcard triples, that have the issue
        if (!statement.contains("vcard")){
            return statement;
        }

        String[] parts = statement.split(" ");

        // vcard itself creates some own ids that are shorter than ids generated by any23
        // but these are not a problem here, as they are connected to longer id, that is then connected to domain
        if (parts[0].length() < 25){
            return statement;
        }

        if (parts[0].contains(":node")){
            // Replace _:node with key
            String[] keyParts = key.split("::");

            // remove datetime
            keyParts[4] = "";


            if (keyParts[3] == "null")
                keyParts[3] = "";

            String url = String.join("", keyParts);

            parts[0] = "<" + url + ">";
            statement = String.join(" ", parts);
        }

        return statement;
    }

    private String fixTriple(String key, String statement){
        // This here parses the extracted triples to make sure they are all correct ones
        // breaks when a triple is not according to standard, then nothing is returned
        try {
            OutputStream outputStream = new ByteArrayOutputStream();

            RDFWriter rdfWriter = Rio.createWriter(RDFFormat.NTRIPLES, outputStream);

            RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);

            ParserConfig parserConfig = new ParserConfig();
            parserConfig.set(BasicParserSettings.PRESERVE_BNODE_IDS, true);

            // MUST BE TRUE for neo4j
            // IRI includes string escapes: '\34' https://openrdf.atlassian.net/browse/SES-2390
            parserConfig.set(BasicParserSettings.VERIFY_URI_SYNTAX, true);

            rdfParser.setParserConfig(parserConfig);
            rdfParser.setRDFHandler(rdfWriter);

            rdfParser.parse(new StringReader(statement), "");

            return outputStream.toString().replaceAll("\n", "");
        } catch (IOException e) {
            logger.error("IOException when parsing " + key);
            e.printStackTrace();
        } catch (RDFParseException e) {
            logger.error("RDFParseException when parsing " + key);
//                e.printStackTrace();

        } catch (UnsupportedRDFormatException e) {
            logger.error("UnsupportedRDFormatException when parsing " + key);
            logger.error(statement);
        } catch (Exception e) {
            logger.error("Exception when parsing " + key);
            e.printStackTrace();
        }

        // TODO return null and check the return value?
        return "";
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
