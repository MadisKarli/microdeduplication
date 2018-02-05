package ee.thesis.processes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class RDFStatistics {

    private static final Logger logger = LogManager.getLogger(RDFStatistics.class);

    @SuppressWarnings("serial")
    public static void countGroups(String[] args) {
        if (args.length < 2) {
            System.err.println("InputFile: NTriples <file>\nOutputFile: File Path");
            System.exit(1);
        }


        SparkConf sparkConf = new SparkConf().setAppName("NTriples To Entity");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        Configuration conf = new Configuration(ctx.hadoopConfiguration());
        // Why didnt i see this sooner?
//        conf.set("textinputformat.record.delimiter", "22-rdf-syntax-ns#type>, ");


        JavaRDD<String> nokid = ctx.textFile(args[0]);

        JavaPairRDD<String, Integer> groups = nokid
                .filter(new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String s) {
                        return s.split(">, <").length == 4;
                    }
                })
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        s = s.substring(1, s.length() - 1);
                        String[] s0 = s.split(">, <");

                        assert s0.length == 4;

                        return new Tuple2<>(s0[2], 1);
                    }
                });

        groups.cache();

        JavaPairRDD<String, Integer> allCounts = groups.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        })
                // Reverse mapping for sort
                .mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) {
                        return new Tuple2(tuple2._2, tuple2._1);
                    }
                })
                .sortByKey(false)
                .mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) {
                        return new Tuple2<>(tuple2._2, tuple2._1);
                    }
                });


        // Sort by more general type
        JavaPairRDD<String, Integer> generalTypes = groups
                .mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {

                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) {
                        String key = tuple2._1;

                        if (key.contains("w3.org")) {
                            key = key.substring(0, key.lastIndexOf("#"));
                            return new Tuple2<>(key, 1);
                        } else if (key.contains("schema.org")) {
                            key = key.substring(0, key.lastIndexOf("/"));
                            return new Tuple2<>(key, 1);
                        } else if (key.contains("data-vocabulary")) {
                            key = key.substring(0, key.lastIndexOf("/"));
                            return new Tuple2<>(key, 1);
                        } else if (key.contains("purl.org")) {
                            return new Tuple2<>(key, 1);
                        } else {
                            logger.error("Unknown RDF url "  + key);
                        }
                        return new Tuple2<>("I dont know how to generalize this url, but it is included in detailed results", 1);
                    }
                })
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) {
                        return integer + integer2;
                    }
                })// Reverse mapping for sort
                .mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) {
                        return new Tuple2(tuple2._2, tuple2._1);
                    }
                })
                .sortByKey(false)
                .mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) {
                        return new Tuple2<>(tuple2._2, tuple2._1);
                    }
                });


        allCounts.coalesce(1).saveAsHadoopFile(args[1] + "/detailed_file_types", String.class, Integer.class, TextOutputFormat.class);
        generalTypes.coalesce(1).saveAsHadoopFile(args[1] + "/general_file_types", String.class, Integer.class, TextOutputFormat.class);
    }
}
