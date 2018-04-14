package ee.microdeduplication.processWarcFiles.utils.spark;

import ee.microdeduplication.processWarcFiles.utils.MicroDataExtraction;
import org.apache.hadoop.io.Text;
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
public class ExtractMicrodataPairFlatMapFunction implements PairFlatMapFunction<Tuple5<String, String, Integer, String, Integer>, Text, Text> {

    private static final Logger logger = LogManager.getLogger(ExtractMicrodataPairFlatMapFunction.class);

    // url, mime/type, size, exception(s), # of triples
    @Override
    public Iterable<Tuple2<Text, Text>> call(Tuple5<String, String, Integer, String, Integer> tuple) {

        List out = new ArrayList<Tuple2<String, String>>();

        // this is a guard statement so that we do not need to split the df into two
        if (tuple._5() != -2)
            return out;

        // Don't parse empty files
        if (tuple._4().length() == 0)
            return out;

        MicroDataExtraction microDataExtraction = new MicroDataExtraction(new ArrayList<String>());

        String nTriples = microDataExtraction.extractMicroData(tuple._1(), tuple._4());

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
            out.add(new Tuple2(new Text(tuple._1()), new Text(statement)));
        }

        return out;
    }
}
