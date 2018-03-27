package ee.microdeduplication.processWarcFiles;

import com.twitter.chill.Tuple4Serializer;
import ee.microdeduplication.processWarcFiles.utils.spark.ExtractMicrodataPairFlatMapFunction;
import ee.microdeduplication.processWarcFiles.utils.spark.IgnoreFunction;
import ee.microdeduplication.processWarcFiles.utils.spark.MapToPairFunction;
import nl.surfsara.warcutils.WarcInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.jwat.warc.WarcRecord;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

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

        logger.info("run time: " + String.valueOf(end - start));
    }


    /*
     * Read in warc files and extract ntriples
     * @param string warcFileDirectoryPath - path to directory containing warc files
     * @param string nTriplesDirectoryPath - path for output directory
     */
    public static void sparkNtripleExtractor(String warcFileDirectoryPath, String nTriplesDirectoryPath) {

        // Initialise Spark
        SparkConf sparkConf = new SparkConf().setAppName("metadata extractor");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Configuration hadoopconf = new Configuration();

        // against java.io.IOException: Filesystem closed
        hadoopconf.setBoolean("fs.hdfs.impl.disable.cache", true);

        // Read in all warc records
        JavaNewHadoopRDD<LongWritable, WarcRecord> warcRecords =
                (JavaNewHadoopRDD<LongWritable, WarcRecord>) sc.newAPIHadoopFile(warcFileDirectoryPath, WarcInputFormat.class, LongWritable.class, WarcRecord.class, hadoopconf);

        // url, mime/type, size, exception(s), # of triples
        // Ignore metadata files - they are big (1 GB) and reading them takes too much time
//        JavaPairRDD<String, String, Integer, String, Integer> warcsWithoutMetadata =
//                warcRecords
//                        .mapPartitionsWithInputSplit(new IgnoreFunction(), false)
//                        .mapToPair(new MapToPairFunction());

        JavaRDD<Tuple5<String, String, Integer, String, Integer>> warcsWithoutMetadata = warcRecords.mapPartitionsWithInputSplit(new IgnoreFunction(), false);


        // Extract ntriples from warc files
        JavaRDD<Tuple5<String, String, Integer, String, Integer>> ntriples =
                warcsWithoutMetadata
                        .map(new ExtractMicrodataPairFlatMapFunction());

        ntriples.saveAsTextFile(nTriplesDirectoryPath);

        int triples = 0;

        for (Tuple5 i: ntriples.collect()){
            int cnt2 = (Integer) i._5();
            if (cnt2 > 0){
                triples += cnt2;
            }
        }
        logger.error("total triples");
        logger.error(triples);


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
