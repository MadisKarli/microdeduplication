package ee.microdeduplication.processWarcFiles.utils.spark;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
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
public class WarcTypeFilter implements Function2<InputSplit, Iterator<Tuple2<LongWritable, WarcRecord>>, Iterator<Tuple2<String, Integer>>> {

    private static final Logger logger = LogManager.getLogger(WarcTypeFilter.class);

    // file size that should be skipped - in octets
    // was 100000000 failed with 2017/08, 8 gb of ram
    // was 13000000, failed with 2017/08 - stackoverflow
    // at ee.microdeduplication.processWarcFiles.Application.sparkNtripleExtractor(Application.java:95)
    // at org.apache.any23.extractor.microdata.MicrodataParser$1.acceptNode(MicrodataParser.java:372)
    // at org.apache.xerces.dom.TreeWalkerImpl.acceptNode(Unknown Source)
    // at org.apache.xerces.dom.TreeWalkerImpl.getNextSibling(Unknown Source)
    // was 5000000, failed with 2017/08 and 10 gb of ram
    private static final int SIZE_LIIMT = 1000000;


    public Iterator<Tuple2<String, Integer>> call(InputSplit arg0,
                                                 Iterator<Tuple2<LongWritable, WarcRecord>> dataIterator) throws Exception {

        FileSplit fileSplit = (FileSplit) arg0;

        //Retrieve the file name from the split
        String fileLocation = fileSplit.getPath().toString();

        List<Tuple2<String, String>> retList = new LinkedList<Tuple2<String, String>>();
        List<Tuple2<String, Integer>> skipcounts = new LinkedList<Tuple2<String, Integer>>();

        String[] nameParts = fileLocation.split("/");

        if (nameParts[nameParts.length - 1].contains("metadata")) {
            logger.debug("igore metadata file " + fileLocation);
            return skipcounts.iterator();
        }

        while (dataIterator.hasNext()) {

            logger.info("processing " + fileLocation);

            Tuple2<LongWritable, WarcRecord> next = dataIterator.next();
            WarcRecord warcRecord = next._2;

            // Filter warcRecords that contain crawl data
            Tuple2<String, Integer> f1 = filter(warcRecord);
            int f = f1._2;
            if (f == 2){
                continue;
            }
            else if (f == 1){
                skipcounts.add(f1);
            }

            String payload = "";

//            try {
//                InputStream payloadStream = warcRecord.getPayload().getInputStream();
//
//                payload = IOUtils.toString(payloadStream);
//
//                payloadStream.close();
//
//
//                // Construct the ID as it was in nutch, example:
//                // http::g.delfi.ee::/s/img/back_grey.gif::null::20150214090921
//                URL url = new URL(warcRecord.getHeader("WARC-Target-URI").value);
//                String protocol = url.getProtocol();
//                String hostname = url.getHost();
//                String urlpath = url.getPath();
//                String param = url.getQuery();
//
//                String dateString = warcRecord.getHeader("WARC-Date").value;
//                dateString = dateString.replaceAll("-|T|Z|:", "");
//
//                String id = protocol + "::" + hostname + "::" + urlpath + "::" + param + "::" + dateString;
//
//                retList.add(new Tuple2<String, String>(id, payload));
//            } catch (NullPointerException e) {
//                retList.add(new Tuple2<String, String>("null", payload));
//            } catch (MalformedURLException e) {
//                retList.add(new Tuple2<String, String>("null", payload));
//            } catch (OutOfMemoryError e) {
//                logger.error("OutOfMemoryError when processing " + fileLocation);
//                logger.error(e.getMessage());
//                return skipcounts.iterator();
//            } catch (Exception e){
//                logger.error("General Exception when processing " + fileLocation);
//                logger.error(e.getMessage());
//            }
        }
        return skipcounts.iterator();
//        return retList.iterator();
    }

    private static Tuple2<String, Integer> filter(WarcRecord warcRecord) {
        String header = "";
        String uri = "";
        try {
            header = warcRecord.getHeader("Content-Type").value;
            uri = warcRecord.getHeader("WARC-Target-URI").value.toLowerCase();
        } catch (NullPointerException e) {
            return new Tuple2(uri, 0);
        }

        // Ignore filetypes that do not contain microdata
        if (filetypeWithoutMetadata(uri))
            return new Tuple2(uri, 0);

        // Ignore WARC specific content and DNS files
        if (header.equals("application/warc-fields")) return new Tuple2(uri, 0);

        if (header.equals("text/dns")) return new Tuple2(uri, 0);

        if (!header.contains("application/http")) return new Tuple2(uri, 0);


        // Ignore warcs that are bigger than 100 megabytes - they tend to run out of memory
        // too much, should be decreased as executors are still killed with it --executor-memory 6g
        String len_s = warcRecord.getHeader("Content-Length").value;
        int len;

        try {
            len = Integer.valueOf(len_s);
        } catch (NumberFormatException e) {
            System.out.println(len_s + " " + uri);
            return new Tuple2(uri, 1);
        }

        if (len > SIZE_LIIMT) {
            System.out.println(len + " " + uri);
            return new Tuple2(uri, 1);
        }

//        for (HeaderLine hl: warcRecord.getHeaderList()){
//            System.out.println(hl.name + " " + hl.value);
//            System.out.println("---------------------------");
//        }

        return new Tuple2(uri, 2);
    }

    private static boolean filetypeWithoutMetadata(String uri){
        List<String> unwantedExtensions = new LinkedList<String>();

        unwantedExtensions.add(".jpg");
        unwantedExtensions.add(".jpeg");
        unwantedExtensions.add(".png");
        unwantedExtensions.add(".gif");
        unwantedExtensions.add(".pdf");
        unwantedExtensions.add(".doc");
        unwantedExtensions.add(".docx");
        unwantedExtensions.add(".flv");
        unwantedExtensions.add(".mp4");
        unwantedExtensions.add(".mpeg");
        unwantedExtensions.add(".mp3");
        unwantedExtensions.add(".wav");
        unwantedExtensions.add(".zip");
        unwantedExtensions.add(".js");
        unwantedExtensions.add(".svg");
        unwantedExtensions.add(".psd");
        unwantedExtensions.add(".css");
        unwantedExtensions.add(".exe");
        unwantedExtensions.add(".webm");
        unwantedExtensions.add(".bmp");
        unwantedExtensions.add(".mpg");
        unwantedExtensions.add(".ogv");
        unwantedExtensions.add(".m4v");
        unwantedExtensions.add(".ttf");

        for (String s: unwantedExtensions){
            if (uri.endsWith(s))
                    return true;
        }

        return false;
    }
}


