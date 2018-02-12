package ee.microdeduplication.processWarcFiles;

import ee.microdeduplication.processWarcFiles.utils.spark.ExtractMicrodataPairFlatMapFunction;
import ee.microdeduplication.processWarcFiles.utils.spark.WarcTypeFilter;
import ee.microdeduplication.processWarcFiles.utils.spark.MapToPairFunction;
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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.jwat.warc.WarcRecord;
import scala.Tuple2;

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

        // Ignore metadata files - they are big (1 GB) and reading them takes too much time
        JavaRDD<Tuple2<String, Integer>> warcsWithoutMetadata =
                warcRecords
                        .mapPartitionsWithInputSplit(new WarcTypeFilter(), false);


        warcsWithoutMetadata.cache();

        List<String> skippedSites = warcsWithoutMetadata.map(new Function<Tuple2<String,Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> stringIntegerTuple2) {
                return stringIntegerTuple2._1;
            }
        }).collect();


        System.out.println("Sites that were skipped ");
        for (String s: skippedSites)
            System.out.println(s);


        Integer skipCount = warcsWithoutMetadata.map(new Function<Tuple2<String, Integer>, Integer>() {
            @Override
            public Integer call(Tuple2<String, Integer> t2) {
                return t2._2;
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        });

        System.out.println("total skipped: " + skipCount);

        warcsWithoutMetadata.coalesce(1).saveAsTextFile(nTriplesDirectoryPath);

//        maybe not needed at all?
//        warcsWithoutMetadata.persist(StorageLevel.DISK_ONLY());
//
//        // Extract ntriples from warc files
//        JavaPairRDD<String, String> ntriples =
//                warcsWithoutMetadata
//                        .flatMapToPair(new ExtractMicrodataPairFlatMapFunction());
//
//        ntriples.saveAsNewAPIHadoopFile(nTriplesDirectoryPath, Text.class, Text.class, org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class, hadoopconf);

        sc.close();
    }
}
