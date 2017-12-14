package ee.microdeduplication.processWarcFiles.utils.spark;

import ee.microdeduplication.processWarcFiles.utils.MicroDataExtraction;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * Created by Madis-Karli Koppel on 15/12/2017.
 */
public class ExtractMicrodataFlatMapFunction implements FlatMapFunction<Tuple2<String, String>, String> {

    private static final Logger logger = LogManager.getLogger(ExtractMicrodataFlatMapFunction.class);

    public Iterable<String> call(Tuple2<String, String> tuple2) throws Exception {

        MicroDataExtraction microDataExtraction = new MicroDataExtraction(new ArrayList<String>());

        try {
            String nTriples = microDataExtraction.extractMicroData(tuple2._2);

            microDataExtraction.setStatements(tuple2._1, nTriples);

            logger.debug("extracted statements " + microDataExtraction.getStatements());
        } catch (Exception e) {
            return new ArrayList();
        }

        return microDataExtraction.getStatements();
    }
}
