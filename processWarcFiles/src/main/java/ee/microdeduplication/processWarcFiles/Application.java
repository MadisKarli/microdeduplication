package ee.microdeduplication.processWarcFiles;

import ee.microdeduplication.processWarcFiles.utils.spark.ExtractMicrodataPairFlatMapFunction;
import ee.microdeduplication.processWarcFiles.utils.spark.FixHTMLPairFlatMapFunction;
import ee.microdeduplication.processWarcFiles.utils.spark.IgnoreFunction;
import nl.surfsara.warcutils.WarcInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jwat.warc.WarcRecord;
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
        //String nTriplesDirectoryPath = args[1] + System.currentTimeMillis();
        String nTriplesDirectoryPath = args[1];

        sparkNtripleExtractor(seqFileDirectoryPath, nTriplesDirectoryPath);

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

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        // Read in all warc records
        JavaNewHadoopRDD<LongWritable, WarcRecord> warcRecords =
                (JavaNewHadoopRDD<LongWritable, WarcRecord>) sc.newAPIHadoopFile(warcFileDirectoryPath, WarcInputFormat.class, LongWritable.class, WarcRecord.class, hadoopconf);

        // Ignore metadata files and thenn call IgnoreFunction for more specific filtering
        JavaRDD<Tuple5<String, String, Integer, String, Integer>> warcsWithoutMetadata = warcRecords
                .filter(line -> {
                    try {
                        return !line._2.header.warcFilename.contains("metadata");
                    } catch (NullPointerException e) {
                        return true;
                    }
                })
                .map(new IgnoreFunction());

        // Extract ntriples from warc files
        JavaPairRDD<String, String> fixedHTML =
                warcsWithoutMetadata
                        .mapToPair(new FixHTMLPairFlatMapFunction());


        // Extract ntriples from warc files
        JavaRDD<Text> ntriples =
                fixedHTML
                        .flatMap(new ExtractMicrodataPairFlatMapFunction());

        //logger.error(ntriples.collect());

        //ntriples.saveAsNewAPIHadoopFile(nTriplesDirectoryPath, Text.class, Text.class, org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class, hadoopconf);
        ntriples.saveAsTextFile(nTriplesDirectoryPath);

        sc.close();
    }
}
