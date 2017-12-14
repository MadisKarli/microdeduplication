package ee.thesis.processSeqFiles;

import ee.thesis.processSeqFiles.models.KeyValue;
import ee.thesis.processSeqFiles.utils.FilesUtil;
import ee.thesis.processSeqFiles.utils.MicroDataExtraction;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.List;



public class Application {

    private static final Logger logger = LogManager.getLogger(Application.class);

    public static void main(String[] args) {

        long start = System.currentTimeMillis();

        if (args.length < 2) {
            System.out.println("Please enter paths to read and write file(s)");
            return;
        }

        String seqFileDirectoryPath = args[0];//"D:\\Docs\\Thesis\\SequenceFiles";
        String nTriplesDirectoryPath = args[1];//"D:\\Docs\\Thesis\\NTriples";



        sparkVersion(seqFileDirectoryPath, nTriplesDirectoryPath+"spark");

        //oldVersion(seqFileDirectoryPath, nTriplesDirectoryPath+"old");

        long end = System.currentTimeMillis();

        logger.error("time: " + String.valueOf(end-start));
    }


    public static void sparkVersion(String seqFileDirectoryPath, String nTriplesDirectoryPath){

        MicroDataExtraction microDataExtraction = new MicroDataExtraction(new ArrayList<>());;

        // Initialise Spark
        SparkConf sparkConf = new SparkConf().setAppName("metadata extractor");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Configuration hadoopconf = new Configuration();


        //Read in all warc records
        JavaPairRDD<Text, Text> warcRecords = sc.newAPIHadoopFile(seqFileDirectoryPath, SequenceFileInputFormat.class, Text.class, Text.class, hadoopconf);

        JavaRDD<String> ntriples = warcRecords.flatMap(new FlatMapFunction<Tuple2<Text, Text>, String>() {
            @Override
            public Iterable<String> call(Tuple2<Text, Text> tuple2) throws Exception {
                try {
                    //String payload =  IOUtils.toString(tuple2._2.getPayload().getInputStream(), StandardCharsets.UTF_8);

                    String nTriples =
                            microDataExtraction.extractMicroData(tuple2._2.toString());

                    microDataExtraction.setStatements(tuple2._1.toString(), nTriples);
                    // logger.debug(microDataExtractionSpark.getStatements());

                } catch (Exception e) {
                    return new ArrayList();
                }
                return microDataExtraction.getStatements();
            }
        });


        ntriples.saveAsTextFile(nTriplesDirectoryPath);

        sc.close();
    }


    public static void oldVersion(String seqFileDirectoryPath, String nTriplesDirectoryPath) {

        MicroDataExtraction microDataExtraction;
        List<KeyValue> keyValueList;
        List<String> statementList;

        File seqFileDirectory = new File(seqFileDirectoryPath);
        try {
            int fileCount = 0;
            File[] filesList = seqFileDirectory.listFiles();
            String csvFilePath;
            File csvFile;
            //for (File seqFile : filesList) {
            for (int k = 0; k < filesList.length; k++) {
                File seqFile = filesList[k];

                csvFilePath = new StringBuilder(nTriplesDirectoryPath)
                        .append("\\").append(seqFile.getName().toString())
                        .append(".csv").toString();
                csvFile = new File(csvFilePath);
                if (csvFile.exists()) {
                    logger.info("Already Exist: file://" + csvFilePath);
                    continue;
                }
                String filePath = seqFile.getPath();
                String fileExtension = FilenameUtils.getExtension(filePath);
                logger.info(fileExtension);
                if (!fileExtension.equals("seq"))
                    continue;
                microDataExtraction = new MicroDataExtraction(new ArrayList<>());
                try {
                    keyValueList = FilesUtil.readSequenceFile(filePath);

                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
                fileCount++;

                for (int i = 0; i < keyValueList.size(); i++) {
                    KeyValue keyValue = keyValueList.get(i);
                    try {
                        String nTriples =
                                microDataExtraction.extractMicroData(keyValue.getValue());

                        microDataExtraction.setStatements(keyValue.getKey(), nTriples);
                    } catch (Exception e) {
                        continue;
                    }
                }
                statementList = microDataExtraction.getStatements();
                logger.info(csvFilePath);
                microDataExtraction.writeToFile(csvFile, statementList);
                logger.info("Total Sequence Files Read: " + fileCount);
            }
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

}
