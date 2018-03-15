package ee.microdeduplication.processWarcFiles.utils.spark;

import ee.microdeduplication.processWarcFiles.utils.MicroDataExtraction;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple5;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Madis-Karli Koppel on 15/12/2017.
 */
public class ExtractMicrodataPairFlatMapFunction implements Function<Tuple5<String, String, Integer, String, Integer>, Tuple5<String, String, Integer, String, Integer>> {

    private static final Logger logger = LogManager.getLogger(ExtractMicrodataPairFlatMapFunction.class);

    // url, mime/type, size, exception(s), # of triples
    @Override
    public Tuple5<String, String, Integer, String, Integer> call(Tuple5<String, String, Integer, String, Integer> tuple) {

        // this is a guard statement so that we do not need to split the df into two
        if (tuple._5() != -2){
            return new Tuple5(tuple._1(), tuple._2(), tuple._3(), tuple._4(), -1);
        }

        MicroDataExtraction microDataExtraction = new MicroDataExtraction(new ArrayList<String>());

        String nTriples = microDataExtraction.extractMicroData(tuple._1(), tuple._4());

        if (nTriples.startsWith("EXCEPTION:")){
            return new Tuple5(tuple._1(), tuple._2(), tuple._3(), nTriples.replace("EXCEPTION:", ""), -1);
        }

        try {
            microDataExtraction.setStatements(tuple._1(), nTriples);
        } catch (ArrayIndexOutOfBoundsException e){
            return new Tuple5(tuple._1(), tuple._2(), tuple._3(), "ArrayIndexOutOfBoundsException when creating NQuads", -1);
        } catch (Exception e){
            return new Tuple5(tuple._1(), tuple._2(), tuple._3(), e.getMessage() + " when creating NQuads", -1);
        }

        logger.debug("extracted statements " + microDataExtraction.getStatements());

        List<String> statements = microDataExtraction.getStatements();

        return new Tuple5(tuple._1(), tuple._2(), tuple._3(), "NONE", statements.size());
    }
}
