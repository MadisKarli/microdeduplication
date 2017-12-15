package ee.thesis.processSeqFiles;

import ee.thesis.processSeqFiles.utils.spark.ExtractMicrodataFlatMapFunction;
import ee.thesis.processSeqFiles.utils.spark.IgnoreMetadataFunction;
import ee.thesis.processSeqFiles.utils.spark.MapToPairFunction;
import nl.surfsara.warcutils.WarcInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jwat.warc.WarcRecord;


/*
* Created by Madis-Karli Koppel and Khalil Rehman
* Extracts ntriples (n-quads) from WARC (web crawling) files
* Uses Apache Any23
* Ignores warc metadata files
* @param input directory containing warc files
* @param output directory for ntriples
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
        String nTriplesDirectoryPath = args[1] + String.valueOf(System.currentTimeMillis());

        sparkNtripleExtractor(seqFileDirectoryPath, nTriplesDirectoryPath + "spark");

        long end = System.currentTimeMillis();

        logger.error("time: " + String.valueOf(end - start));
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

        // Ignore metadata files - they are big (1 GB) and reading them takes too much time
        JavaPairRDD<String, String> warcsWithoutMetadata =
                warcRecords
                        .mapPartitionsWithInputSplit(new IgnoreMetadataFunction(), true)
                        .mapToPair(new MapToPairFunction());


        // Extract ntriples from warc files
        JavaRDD<String> ntriples =
                warcsWithoutMetadata.
                        flatMap(new ExtractMicrodataFlatMapFunction());

        ntriples.saveAsTextFile(nTriplesDirectoryPath);

        sc.close();
    }
}
