package ee.thesis;

import ee.thesis.processes.RDFStatistics;
import org.apache.commons.lang.ArrayUtils;

import ee.thesis.processes.ConvertionToEntities;
import ee.thesis.processes.Deduplication;
import ee.thesis.processes.EvaluateDeduplication;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Application {

    private static final Logger logger = LogManager.getLogger(Application.class);

    public static void main(String[] args){
        if (!(args.length > 0)) {
            System.err.println("Usage: Process <entityformation, deduplication, or evaluation>."
                    + "  \nInput File <file>.\nOutput <file>.");
            System.exit(1);
        }

        long start = System.currentTimeMillis();

        try{
            if(args[0].trim().toLowerCase().equals("entityformation")){
                args=(String[]) ArrayUtils.removeElement(args, args[0]);
                ConvertionToEntities.convertToEntities(args);
            }
            else if(args[0].trim().toLowerCase().equals("deduplication")){
                args=(String[]) ArrayUtils.removeElement(args, args[0]);
                Deduplication.Deduplicate(args);
            }
            else if(args[0].trim().toLowerCase().equals("evaluation")){
                args=(String[]) ArrayUtils.removeElement(args, args[0]);
                EvaluateDeduplication.evaluate(args);
            }
            else if(args[0].trim().toLowerCase().equals("statistics")){
                args=(String[]) ArrayUtils.removeElement(args, args[0]);
                RDFStatistics.countGroups(args);
            }
        }catch(Exception ex){
            System.out.println(ex.getMessage());
        }
        long end = System.currentTimeMillis();
        System.out.println("time taken: " + String.valueOf(end - start));
    }

}
