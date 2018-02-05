package ee.microdeduplication.processWarcFiles.utils.spark;

import ee.microdeduplication.processWarcFiles.utils.MicroDataExtraction;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Madis-Karli Koppel on 15/12/2017.
 */
public class ExtractMicrodataPairFlatMapFunction implements PairFlatMapFunction<Tuple2<String, String>, String, String> {

    private static final Logger logger = LogManager.getLogger(ExtractMicrodataPairFlatMapFunction.class);


    @Override
    public Iterable<Tuple2<String, String>> call(Tuple2<String, String> tuple2)  {
        MicroDataExtraction microDataExtraction = new MicroDataExtraction(new ArrayList<String>());

        try {
            String nTriples = microDataExtraction.extractMicroData(tuple2._2);

            microDataExtraction.setStatements(tuple2._1, nTriples);

            logger.debug("extracted statements " + microDataExtraction.getStatements());
        } catch (Exception e) {
            return new ArrayList<Tuple2<String, String>>();
        }

        List out = new ArrayList<Tuple2<String, String>>();

        for (String statement: microDataExtraction.getStatements()){
            out.add(new Tuple2(tuple2._1, statement));
        }

        return out;
    }
}
