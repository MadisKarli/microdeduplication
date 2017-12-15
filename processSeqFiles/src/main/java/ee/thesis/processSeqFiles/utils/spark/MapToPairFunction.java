package ee.thesis.processSeqFiles.utils.spark;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by Madis-Karli Koppel on 15/12/2017.
 */
public class MapToPairFunction implements PairFunction<Tuple2<String, String>, String, String> {
    public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
        return new Tuple2<String, String>(t._1, t._2);
    }
}
