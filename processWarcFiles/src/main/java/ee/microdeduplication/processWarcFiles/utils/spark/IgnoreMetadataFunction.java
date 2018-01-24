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

            Tuple2<LongWritable, WarcRecord> next = dataIterator.next();
            WarcRecord warcRecord = next._2;

            // Filter warcRecords that contain crawl data, no win in time
//            if(!filter(warcRecord))
//                continue;

            String payload = IOUtils.toString(warcRecord.getPayload().getInputStream());

            try {
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
            }
        }
        return retList.iterator();
    }

    private static boolean filter(WarcRecord warcRecord) {
        String header = "";
        try {
            header = warcRecord.getHeader("Content-Type").value;
        } catch (NullPointerException e) {
            return false;
        }

        // Ignore WARC specific content and DNS files
        if (header.equals("application/warc-fields")) return false;

        if (header.equals("text/dns")) return false;

        return true;
    }
}


