package ee.microdeduplication.processWarcFiles.utils;


import org.apache.any23.Any23;
import org.apache.any23.source.DocumentSource;
import org.apache.any23.source.StringDocumentSource;
import org.apache.any23.writer.NTriplesWriter;
import org.apache.any23.writer.TripleHandler;
import org.apache.any23.writer.TripleHandlerException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
    public String extractMicroData(String contents) throws TripleHandlerException, IOException {

        logger.debug("In extracting microdata.");

        Any23 runner = new Any23("html-microdata");

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

            logger.debug(result);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } catch (Error e) {
            logger.error(e.getMessage());
        }

        // TODO is the try needed at all?
        try {
            // This needs to be here, not in try, otherwise some triples are lost
            // store any23 results in result String, ntriples are separated by newline
            result = out.toString();

        } catch (Exception e) {
            logger.error(e.getMessage());
        } catch (Error e) {
            logger.error(e.getMessage());
        }

        out.close();
        handler.close();

        if (!result.isEmpty()) {
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
     * Convert ntiples into n-quads
     * n-quads are stored in field statementsList
     * @param string key - the key from warc file containing triple location and date of extraction
     * @param string nTriples - ntriples, several, separated by new line
     */
    public void setStatements(String key, String nTriples) {
        String[] statements = nTriples.split("(\\s\\.)(\\r?\\n)");
        StringBuilder stat;

        for (String statement : statements) {

            stat = new StringBuilder("");

            String[] statParts = statement.split("\\s(<|\"|_)");

            String subject = statParts[0].replaceAll("(<|>|\")", "");
            String predicate = statParts[1].replaceAll("(<|>|\")", "");
            String object = statParts[2].replaceAll("(<|>|\")", "");

            stat.append("<" + key + ">, ").append("<" + subject + ">, ")
                    .append("<" + predicate + ">, ").append("<" + object + ">");

            statementsList.add(stat.toString());

            logger.debug(statement);
            logger.debug(stat.toString());
        }
    }

    public MicroDataExtraction(List<String> statementsList) {
        this.statementsList = statementsList;
    }

    public List<String> getStatements() {
        return statementsList;
    }

}
