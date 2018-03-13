package ee.microdeduplication.processWarcFiles.utils.spark;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import org.jwat.common.HeaderLine;
import org.jwat.warc.WarcRecord;
import scala.Tuple2;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Madis-Karli Koppel on 15/12/2017.
 * inspired by http://baahu.in/spark-how-to-get-the-file-name-for-a-record-of-an-rdd/
 * filter the filename to ignore metadata files
 * these just kept running for hours and I did not find a better way to filter them
 */
public class IgnoreMetadataFunction implements Function2<InputSplit, Iterator<Tuple2<LongWritable, WarcRecord>>, Iterator<Tuple2<String, String>>> {

    private static final Logger logger = LogManager.getLogger(IgnoreMetadataFunction.class);

    private static final int LEN_LIMIT_TEXT = 13000000;
    private static final int LEN_LIMIT_OTHER = 0;


    public Iterator<Tuple2<String, String>> call(InputSplit arg0,
                                                 Iterator<Tuple2<LongWritable, WarcRecord>> dataIterator) throws Exception {

        FileSplit fileSplit = (FileSplit) arg0;

        //Retrieve the file name from the split
        String fileLocation = fileSplit.getPath().toString();

        List<Tuple2<String, String>> retList = new LinkedList<Tuple2<String, String>>();

        String[] nameParts = fileLocation.split("/");

        if (nameParts[nameParts.length - 1].contains("metadata")) {
            logger.debug("igore metadata file " + fileLocation);
            return retList.iterator();
        }

        while (dataIterator.hasNext()) {

            logger.info("processing " + fileLocation);

            Tuple2<LongWritable, WarcRecord> next = dataIterator.next();
            WarcRecord warcRecord = next._2;

            // Filter warcRecords that contain crawl data, no win in time
            if (!filterSize(warcRecord))
                continue;

            String payload = "";

            try {
                InputStream payloadStream = warcRecord.getPayload().getInputStream();

                payload = IOUtils.toString(payloadStream);

                payloadStream.close();

                // Construct the ID as it was in nutch, example:
                // http::g.delfi.ee::/s/img/back_grey.gif::null::20150214090921
                URL url = new URL(warcRecord.getHeader("WARC-Target-URI").value);
                String protocol = url.getProtocol();
                String hostname = url.getHost();
                String urlpath = url.getPath();
                String param = url.getQuery();

                String dateString = warcRecord.getHeader("WARC-Date").value;
                dateString = dateString.replaceAll("-|T|Z|:", "");

                String id = protocol + "::" + hostname + "::" + urlpath + "::" + param + "::" + dateString;

                retList.add(new Tuple2<String, String>(id, payload));
            } catch (NullPointerException e) {
                retList.add(new Tuple2<String, String>("null", payload));
            } catch (MalformedURLException e) {
                retList.add(new Tuple2<String, String>("null", payload));
            } catch (OutOfMemoryError e) {
                logger.error("Exception when processing " + fileLocation);
                logger.error(e.getMessage());
            }
        }
        return retList.iterator();
    }

    // TODO refactor as size is no longer checked
    private static boolean filterSize(WarcRecord warcRecord) {
        String header = "";
        try {
            header = warcRecord.getHeader("Content-Type").value;
        } catch (NullPointerException e) {
            return false;
        }


        // Ignore WARC specific content and DNS files
        if (header.equals("application/warc-fields")) return false;

        if (header.equals("text/dns")) return false;

        if (!header.contains("application/http")) return false;

        // Ignore warcs that are too big
        // There are two limits - for text files and for other files
        // Microdata extraction from text files is
        String len_s = warcRecord.getHeader("Content-Length").value;
        int len;

        try {
            len = Integer.valueOf(len_s);
        } catch (NumberFormatException e) {
            return false;
        }

        // Set default target to that of non text files
        int target = LEN_LIMIT_OTHER;

        // Start checking if we can change it to text file target
        // html files can be represented as
        // text/html
        // application/http
        // application/xhtml+xml
        if (header.contains("http")){
            target = LEN_LIMIT_TEXT;
        }

        // XML files also contain info
        // application/xhtml+xml
        // application/rss+xml
        // application/rdf+xml
        if (header.contains("xml")){
            target = LEN_LIMIT_TEXT;
        }

        // json files can contain microdata
        if (header.contains("json")){
            logger.error("json file " + header);
            target = LEN_LIMIT_TEXT;
        }

        // parsing text files is fast and most of the time
        // text/plain has been mistaken for text/html
        // text/html
        if (header.startsWith("text")){
            target = LEN_LIMIT_TEXT;
        }

//        if (len > target) {
//            logger.error("Ignoring Warc record due to size: " + len +"|" + target + " " + header);
//            return false;
//        }

        if (target != LEN_LIMIT_TEXT){
            logger.info("Ignoring Warc record as it is not a text file");
            return false;
        }

        return true;
    }
}
