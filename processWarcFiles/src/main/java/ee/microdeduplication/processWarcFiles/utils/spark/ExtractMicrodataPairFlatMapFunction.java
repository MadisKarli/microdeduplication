package ee.microdeduplication.processWarcFiles.utils.spark;

import ee.microdeduplication.processWarcFiles.utils.MicroDataExtraction;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
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
public class ExtractMicrodataPairFlatMapFunction implements FlatMapFunction<Tuple2<String, String>, Text> {

    private static final Logger logger = LogManager.getLogger(ExtractMicrodataPairFlatMapFunction.class);

    // key, value
    @Override
    public Iterable<Text> call(Tuple2<String, String> tuple) {

        List out = new ArrayList<String>();

        if (tuple == null)
            return out;

        MicroDataExtraction microDataExtraction = new MicroDataExtraction(new ArrayList<String>());

        String nTriples = microDataExtraction.extractMicroData(tuple._1(), tuple._2());

        if (nTriples.startsWith("EXCEPTION:")){
            return out;
        }

        try {
            microDataExtraction.setStatements(tuple._1(), nTriples);
        } catch (ArrayIndexOutOfBoundsException e){
            return out;
        } catch (Exception e){
            return out;
        }

        logger.debug("extracted statements " + microDataExtraction.getStatements());

        for (String statement: microDataExtraction.getStatements()){
            out.add(new Text(statement));
        }

        return out;
    }
}
