package ee.microdeduplication.processWarcFiles;

import ee.microdeduplication.processWarcFiles.utils.spark.ExtractMicrodataPairFlatMapFunction;
import ee.microdeduplication.processWarcFiles.utils.spark.FixHTMLPairFlatMapFunction;
import ee.microdeduplication.processWarcFiles.utils.spark.IgnoreFunction;
import nl.surfsara.warcutils.WarcInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.jwat.warc.WarcRecord;
import scala.Tuple2;
import scala.Tuple5;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/*
 * Created by Khalil Rehman (base) and Madis-Karli Koppel (converted to Spark)
 *
 * @param input directory containing warc files
 * @param output directory for ntriples
 *
 *
 * Extracts ntriples (n-quads) from WARC (web crawling) files
 * Uses Apache Any23
 * Ignores warc metadata files
 *
 * A ntriple (n-quad) contains key, subject, predicate and object
 * full: <http::www.avancia.ee::/juuksehooldus/squarespace.net::null::20150214221751>, <_:node7145b4d244f0db32ed9ae797c299fd1a>, <http://schema.org/WebPage/name>, <Avancia ilusalong@et-ee>
 * key - contains location and crawl date'<http::www.avancia.ee::/juuksehooldus/squarespace.net::null::20150214221751>'
 * subject - '<java:MicroDataExtraction>' or '<_:node7145b4d244f0db32ed9ae797c299fd1a>'
 * predicate - '<http://purl.org/dc/terms/title>'
 * object - '<Avancia ilusalong@et-ee>'
 */
public class Application {

    private static final Logger logger = LogManager.getLogger(Application.class);

    public static void main(String[] args) {

        long start = System.currentTimeMillis();

        if (args.length < 2) {
            System.out.println("Please enter <input directory> and <output directory>");
            return;
        }

        String seqFileDirectoryPath = args[0];
        // TODO remove when final
        String nTriplesDirectoryPath = args[1] + String.valueOf(System.currentTimeMillis());

        sparkNtripleExtractor(seqFileDirectoryPath, nTriplesDirectoryPath + "spark");

        long end = System.currentTimeMillis();

        logger.error("run time: " + String.valueOf(end - start));
    }


    /*
     * Read in warc files and extract ntriples
     * @param string warcFileDirectoryPath - path to directory containing warc files
     * @param string nTriplesDirectoryPath - path for output directory
     */
    public static void sparkNtripleExtractor(String warcFileDirectoryPath, String nTriplesDirectoryPath) {

        Configuration hadoopconf = new Configuration();

        // against java.io.IOException: Filesystem closed
        hadoopconf.setBoolean("fs.hdfs.impl.disable.cache", true);

        SparkConf sparkConf = new SparkConf().setAppName("metadata extractor");
        // none of those works ....
        sparkConf.set("spark.mapred.min.split.size", "536870912");
        sparkConf.set("spark.mapred.max.split.size", "536870912");
        sparkConf.set("mapreduce.input.fileinputformat.split.maxsize", "536870912");
        sparkConf.set("spark.input.fileinputformat.split.maxsize", "536870912");
        sparkConf.set("spark.mapreduce.input.fileinputformat.split.maxsize", "536870912");
        sparkConf.set("spark.mapred.input.fileinputformat.split.maxsize", "536870912");
        sparkConf.set("mapred.max.split.size", "41943040");
        sparkConf.set("mapred.min.split.size", "20971520");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Read in all warc records
        JavaNewHadoopRDD<LongWritable, WarcRecord> warcRecords =
                (JavaNewHadoopRDD<LongWritable, WarcRecord>) sc.newAPIHadoopFile(warcFileDirectoryPath, WarcInputFormat.class, LongWritable.class, WarcRecord.class, hadoopconf);

        // todo no need for that long
        JavaRDD<Tuple2<Long, WarcRecord>> warcRecords2 = warcRecords.mapPartitionsWithInputSplit(new Function2<InputSplit, Iterator<Tuple2<LongWritable, WarcRecord>>, Iterator<Tuple2<Long, WarcRecord>>>() {

            @Override
            public Iterator<Tuple2<Long, WarcRecord>> call(InputSplit arg0, Iterator<Tuple2<LongWritable, WarcRecord>> dataIterator) {

                FileSplit fileSplit = (FileSplit) arg0;

                //Retrieve the file name from the split
                String fileLocation = fileSplit.getPath().toString();

                List<Tuple2<Long, WarcRecord>> retList = new LinkedList<Tuple2<Long, WarcRecord>>();

                String[] nameParts = fileLocation.split("/");

                if (nameParts[nameParts.length - 1].contains("metadata")) {
                    logger.debug("igore metadata file " + fileLocation);
                    return retList.iterator();
                }

                while (dataIterator.hasNext()) {
                    Tuple2<LongWritable, WarcRecord> tuple = dataIterator.next();
                    WarcRecord record = tuple._2;
                    Long key = tuple._1.get();
                    retList.add(new Tuple2<Long, WarcRecord>(key, record));
                }

                return retList.iterator();
            }
        }, false);

        //JavaRDD<Tuple2<LongWritable, WarcRecord>> warcRecords3 = warcRecords2.repartition(300);
        System.out.println(warcRecords2.partitions().size());


        JavaRDD<Tuple5<String, String, Integer, String, Integer>> warcsWithoutMetadata =
                warcRecords2.map(new IgnoreFunction())
                .repartition(300);

        warcsWithoutMetadata.persist(StorageLevel.DISK_ONLY());


        // Extract ntriples from warc files
        JavaPairRDD<String, String> fixedHTML =
                warcsWithoutMetadata
                        .mapToPair(new FixHTMLPairFlatMapFunction());

        // Extract ntriples from warc files
        JavaRDD<Text> ntriples =
                fixedHTML
                        .flatMap(new ExtractMicrodataPairFlatMapFunction());

        //ntriples.saveAsNewAPIHadoopFile(nTriplesDirectoryPath, Text.class, Text.class, org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class, hadoopconf);
        ntriples.saveAsTextFile(nTriplesDirectoryPath);

        // 1032-1-20170821154232650-00002-ciblee_2015_netarchive.warc
        //    438 with exceptions (number of triples)
        // 19 534 without size exception - :O
        // 104-1-20170818022359886-00003-ciblee_2015_netarchive.warc
        // 1032-1-20170821154232650-00002-ciblee_2015_netarchive.warc
        // 1034-1-20170821160700559-00002-ciblee_2015_netarchive.warc
        //  5 077 with exceptions
        // 43 344 without size exception (any 23 1.1)
        // 47 548 without size exception (any 23 2.1)
        // warcs 232
        //     663 with exceptions
        // 161 890 without size exceptions (any 23 1.1)
        // 161 863 without size exceptions (any 23 2.1)

        sc.close();
    }
}
