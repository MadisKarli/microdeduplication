package ee.microdeduplication.processWarcFiles.utils.spark;

import ee.microdeduplication.processWarcFiles.utils.MicroDataExtraction;
import ee.microdeduplication.processWarcFiles.utils.spark.ExtractMicrodataPairFlatMapFunction;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFunction;
import org.ccil.cowan.tagsoup.HTMLSchema;
import org.ccil.cowan.tagsoup.Parser;
import org.ccil.cowan.tagsoup.XMLWriter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import scala.Tuple2;
import scala.Tuple5;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FixHTMLPairFlatMapFunction implements PairFunction<Tuple5<String, String, Integer, String, Integer>, String, String> {
    private static final Logger logger = LogManager.getLogger(ExtractMicrodataPairFlatMapFunction.class);

    // url, mime/type, size, exception(s), # of triples
    @Override
    public Tuple2<String, String> call(Tuple5<String, String, Integer, String, Integer> tuple) {

        // this is a guard statement so that we do not need to split the df into two
        if (tuple._5() != -2)
            return null;

        // Don't parse empty files
        if (tuple._4().length() == 0)
            return null;

        String fixedHTML = fixHtml(tuple._4());

        return new Tuple2(tuple._1(), fixedHTML);

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
}
