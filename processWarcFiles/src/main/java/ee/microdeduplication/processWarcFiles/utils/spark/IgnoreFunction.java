package ee.microdeduplication.processWarcFiles.utils.spark;

import ee.microdeduplication.processWarcFiles.utils.SimpleTuple;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import org.jwat.common.HeaderLine;
import org.jwat.common.HttpHeader;
import org.jwat.warc.WarcRecord;
import scala.Tuple2;
import scala.Tuple5;

import java.io.IOException;
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
public class IgnoreFunction implements Function2<InputSplit, Iterator<Tuple2<LongWritable, WarcRecord>>, Iterator<Tuple5<String, String, Integer, String, Integer>>> {

    private static final Logger logger = LogManager.getLogger(IgnoreFunction.class);

    // 13000000 - 12.4 MB
    // TODO replace with bigger size
    private static final int LEN_LIMIT_TEXT = 13000000;
    private static final int LEN_LIMIT_OTHER = 0;

    public Iterator<Tuple5<String, String, Integer, String, Integer>> call(InputSplit arg0,
                                                 Iterator<Tuple2<LongWritable, WarcRecord>> dataIterator) {

        FileSplit fileSplit = (FileSplit) arg0;

        //Retrieve the file name from the split
        String fileLocation = fileSplit.getPath().toString();

        List<Tuple5<String, String, Integer, String, Integer>> retList = new LinkedList<Tuple5<String, String, Integer, String, Integer>>();

        String[] nameParts = fileLocation.split("/");

        if (nameParts[nameParts.length - 1].contains("metadata")) {
            logger.debug("igore metadata file " + fileLocation);
            retList.add(new Tuple5<String, String, Integer, String, Integer>(fileLocation, "metadata", -1, "metadata", -1));
            return retList.iterator();
        }

        while (dataIterator.hasNext()) {

            logger.info("processing " + fileLocation);

            Tuple2<LongWritable, WarcRecord> next = dataIterator.next();
            WarcRecord warcRecord = next._2;

            String mime;
            Integer size;

            // Filter warcRecords that contain crawl data, no win in time
            // url, mime/type, size, exception(s), # of triples
            SimpleTuple sizeTuple = filterWarcRecord(warcRecord);
            if (sizeTuple.comment != null){
                retList.add(new Tuple5<String, String, Integer, String, Integer>(fileLocation, "UNK", -1, sizeTuple.comment, -1));
                continue;
            }

            HttpHeader httpHeader = null;
            try{
                httpHeader = warcRecord.getHttpHeader();
            } catch (NullPointerException e){
                retList.add(new Tuple5<String, String, Integer, String, Integer>(fileLocation, "UNK", -1, "NullPointerException when extracting Http header", -1));
                continue;
            }

            if (httpHeader == null){
                retList.add(new Tuple5<String, String, Integer, String, Integer>(fileLocation, "UNK", -1, "httpheader is null", -1));
                continue;
            }


            SimpleTuple typeFilter = filterHttpHeader(httpHeader);
            if (typeFilter.comment != null){
                retList.add(new Tuple5<String, String, Integer, String, Integer>(fileLocation, typeFilter.mime, typeFilter.size, typeFilter.comment, -1));
                continue;
            }
            mime = typeFilter.mime;
            size = typeFilter.size;

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

                //TODO check the last number
                retList.add(new Tuple5<String, String, Integer, String, Integer>(id, mime, size, payload, -2));
            } catch (NullPointerException e) {
                // Most likely due to URL url = new URL(warcRecord.getHeader("WARC-Target-URI").value);
                for (HeaderLine l: warcRecord.getHeaderList()){
                    System.out.println(l.name + " " + l.value);
                }
                e.printStackTrace();
                retList.add(new Tuple5<String, String, Integer, String, Integer>(fileLocation, mime, size, "NullPointerException when creating ID", -1));
            } catch (MalformedURLException e) {
                retList.add(new Tuple5<String, String, Integer, String, Integer>(fileLocation, mime, size, "MalformedURLException when creating ID", -1));
            } catch (OutOfMemoryError e) {
//                Exception when processing hdfs://ir-hadoop1:8020/data/webarchive/2017/08/1029-1-20170821151931191-00001-ciblee_2015_netarchive.warc
//                Requested array size exceeds VM limit
                logger.error("Exception when processing " + fileLocation);
                logger.error(e.getMessage());
                retList.add(new Tuple5<String, String, Integer, String, Integer>(fileLocation, mime, size, "OutOfMemoryError when creating ID", -1));
            } catch (IOException e) {
                retList.add(new Tuple5<String, String, Integer, String, Integer>(fileLocation, mime, size, "IOException when creating reading warc record", -1));
            }
        }
        return retList.iterator();
    }

    private static SimpleTuple filterWarcRecord(WarcRecord warcRecord){
        String header = "";
        int len = -1;
        try {
            header = warcRecord.getHeader("Content-Type").value;
        } catch (NullPointerException e) {
            return new SimpleTuple("UNK", len, "NullPointerException when warc record Content-Type");
        }

        // Ignore WARC specific content and DNS files
        if (header.equals("application/warc-fields")) {
            return new SimpleTuple(header, len, "Ignore warc specific content: application/warc-fields");
        }

        if (header.equals("text/dns")) {
            return new SimpleTuple(header, len, "Ignore warc specific content: text/dns");
        }

        if (header.equals("") || header.equals(" ")){
            return new SimpleTuple("empty", len, "Ignore empty Content-Type");
        }

        if (!header.contains("application/http")) {
            logger.error(header);
//            return new SimpleTuple(header, len, "Ignore unk Warc header: " + header);
        }

        return new SimpleTuple(header, len, null);
    }


    private static SimpleTuple filterHttpHeader(HttpHeader httpHeader) {
        String header = "unk";
        int len = -1;

        try {
            header = httpHeader.getHeader("Content-Type").value;
        } catch (NullPointerException e) {
            return new SimpleTuple(header, len, "NullPointerException when extracting Content-Type");
        }


        // Ignore warcs that are too big
        // There are two limits - for text files and for other files
        try {
            String len_s = httpHeader.getHeader("Content-Length").value;
            len = Integer.valueOf(len_s);
        } catch (NumberFormatException e) {
            return new SimpleTuple(header, -1, "NumberFormatException when extracting Content-Length");
        }catch (NullPointerException e) {
            return new SimpleTuple("UNK", len, "NullPointerException when extracting Content-Length");
        }


        // Set default target to that of non text files
        int target = LEN_LIMIT_OTHER;

        // Start checking if we can change it to text file target
        // html files can be represented as
        // text/html
        // they should be
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
            target = LEN_LIMIT_TEXT;
        }

        // parsing text files is fast and most of the time
        // text/plain has been mistaken for text/html
        // text/html
        if (header.startsWith("text")){
            target = LEN_LIMIT_TEXT;
        }

        if (len > target){
            return new SimpleTuple(header, len, "Ignore due to file type size goal " + target);
        }

        return new SimpleTuple(header, len, null);
    }
}
