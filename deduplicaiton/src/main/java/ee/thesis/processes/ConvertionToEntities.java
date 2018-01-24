package ee.thesis.processes;

import java.util.*;

import ee.thesis.Application;
import ee.thesis.models.Entity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import ee.thesis.utils.Util;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import static java.lang.Thread.sleep;

public class ConvertionToEntities {
    private static long count = 1;

    private static final Logger logger = LogManager.getLogger(ConvertionToEntities.class);

    @SuppressWarnings("serial")
    public static void convertToEntities(String[] args) {
        if (args.length < 2) {
            System.err.println("InputFile: NTriples <file>\nOutputFile: File Path");
            System.exit(1);
        }


        SparkConf sparkConf = new SparkConf().setAppName("NTriples To Entity");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        Configuration conf = new Configuration(ctx.hadoopConfiguration());
        conf.set("textinputformat.record.delimiter", "22-rdf-syntax-ns#type>, ");

        JavaPairRDD<LongWritable, Text> lines = ctx.newAPIHadoopFile
                (
                        args[0],
                        TextInputFormat.class,
                        LongWritable.class,
                        Text.class,
                        conf
                );


        JavaRDD<String> bLines = lines.map(new Function<Tuple2<LongWritable, Text>, String>() {
            //@Override
            public String call(Tuple2<LongWritable, Text> line) {
                return line._2.toString();
            }
        }).filter(
                /*line -> line.matches("(.*Product/name.*)"));*/
                new Function<String, Boolean>() {
                    public Boolean call(String bLines) {
                        return (bLines.contains("org/Product") || bLines.contains("org/Offer"));
                    }
                });


        JavaRDD<String> moreEntities2 = lines.map(new Function<Tuple2<LongWritable, Text>, String>() {
            @Override
            public String call(Tuple2<LongWritable, Text> t2) {
//                logger.error(t2._1.toString());
                return t2._2.toString();
            }
        });


        final String[] targets = {
                ".org/Offer", "Product/name", "Offer/itemOffered", "Product/image", "Offer/image", "Offer/price", "Product/price", "Offer/priceCurrency", "Offer/currency", "Product/currency", "Product/description", "Offer/description", "Product/url", "Product/sku", "Offer/availability"
        };

        JavaRDD<String> nokid = ctx.textFile(args[0]);


        JavaPairRDD<String, Tuple3<String, String, String>> ab = nokid
                .filter(new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String s) {
                        return s.contains("schema.org");
//                        return inList(s, targets);
                    }
                })
                .mapToPair(new PairFunction<String, String, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple2<String, Tuple3<String, String, String>> call(String s) {
                        s = s.substring(1, s.length() - 1);
                        String[] s0 = s.split(">, <");

                        assert s0.length == 4;

                        return new Tuple2<>(s0[1], new Tuple3<>(s0[0], s0[2] , s0[3]));
                    }
                });

        ab.groupByKey()
                .map(
                new Function<Tuple2<String,Iterable<Tuple3<String,String,String>>>, Object>() {
                    @Override
                    public Object call(Tuple2<String, Iterable<Tuple3<String, String, String>>> tuple2) {
                        // Structure: id, tuple3
                        // tuple3: siteUrl, RDF type, RDF value ? is RDF correct here?
                        
                        String id = tuple2._1();
                        
                        Iterator<Tuple3<String, String, String>> it = tuple2._2.iterator();

                        SortedSet<String> urls = new TreeSet();
                        Map<String, String> currentFields = new HashMap<String, String>();
                        String type = "";

                        while(it.hasNext()){
                            Tuple3<String, String, String> tuple3 = it.next();

                            urls.add(tuple3._1());

                            String _key = tuple3._2();
                            String key;

                            // TODO make more readable
                            // sometimes they tell us the type of entity then there is no point in parsing ourself
                            // http://www.w3.org/1999/02/22-rdf-syntax-ns#type
                            if (_key.contains("22-rdf-syntax-ns#type")){
                                type = tuple3._3();
                            }
                            else {
                                key = _key.substring(_key.lastIndexOf("/") + 1, _key.length());

                                // checks for repetition - we do not want duplicate keys
                                if (currentFields.containsKey(key)){
//                                    logger.debug("Duplicate RDF type " + key + " in " + tuple3._1()  + ", id=" + id);

                                    // TODO better solution
                                    String old = currentFields.get(key);
                                    currentFields.put(key, old + "," + tuple3._3());
                                } else{
                                    currentFields.put(key, tuple3._3());
                                }
                            }
                        }

                        if (type.isEmpty()){
                            // types are as an URL, example http://schema.org/Product/description
                            String _type = tuple2._2.iterator().next()._2();
                            type = _type.substring(0, _type.lastIndexOf("/"));
                        }

                        
                        // checks that our RDF is from only one URL
                        if (urls.size() != 1) logger.error("too many urls in: " + id + " " + urls);

                        Entity entity = new Entity();
                        entity.url = urls.first();
                        entity.any23id = id;
                        entity.type = type;

                        entity.fields = currentFields;
                        System.out.println(entity);
                        System.out.println("------------");

                        return null;
                    }
                }
        ).collect();


        // moreEntities2 is ok but loses some info

        JavaRDD<String> entity = bLines.map(new Function<String, String>() {

            //@Override
            public String call(String bLines) {

                String name = "";
                String sku = "";
                String description = "";
                String imageUrl = "";
                String productUrl = "";
                String provider = "";
                String price = "";
                String currency = "";
                String timeStamp = "";
                String availability = "";
                String[] lines = bLines.split("\n");
                for (String line : lines) {
                    try {
                        if (bLines.contains(".org/Offer>")) {
                            if (Util.productMap != null) {
                                name = Util.productMap.get("name");
                                sku = Util.productMap.get("sku");
                                description = Util.productMap.get("description");
                                imageUrl = Util.productMap.get("sku");
                                productUrl = Util.productMap.get("productUrl");
                                provider = Util.productMap.get("provider");
                                price = Util.productMap.get("price");
                                currency = Util.productMap.get("currency");
                                timeStamp = Util.productMap.get("timeStamp");
                                availability = Util.productMap.get("availability");
                            }
                            Util.productMap = null;
                        }
                        if (line.contains("Product/name") || line.contains("Offer/itemOffered")) {
                            if (name.isEmpty())
                                name = line.split(">, ")[3];
                            if (timeStamp.isEmpty())
                                timeStamp = getTimeStamp(line);
                            if (provider.isEmpty())
                                provider = getProvider(line);
                        } else if (line.contains("Product/image") || line.contains("Offer/image")) {
                            imageUrl = line.split(">, ")[3];
                        } else if (line.contains("Offer/price") || line.contains("Product/price")) {
                            if (price.isEmpty())
                                price = line.split(">, ")[3];
                            if (timeStamp.isEmpty())
                                timeStamp = getTimeStamp(line);
                            if (provider.isEmpty())
                                provider = getProvider(line);
                        } else if (line.contains("Offer/priceCurrency") || line.contains("Offer/currency")
                                || line.contains("Product/currency")) {
                            if (currency.isEmpty())
                                currency = line.split(">, ")[3];
                        } else if (line.contains("Product/description") || line.contains("Offer/description")) {
                            if (description.isEmpty())
                                description = line.split(">, ")[3];
                        } else if (line.contains("Product/url")) {
                            productUrl = line.split(">, ")[3];
                        } else if (line.contains("Product/sku"))
                            sku = line.split(">, ")[3];
                        else if (line.contains("Offer/availability")) {
                            availability = line.split(">, ")[3];
                        }
                    } catch (Exception e) {
                        continue;
                    }
                }
                String tuple = "";
                if (bLines.contains(".org/Product>")) {
                    HashMap<String, String> pMap = new HashMap<String, String>();
                    pMap = new HashMap<String, String>();
                    pMap.put("name", name);
                    pMap.put("sku", sku);
                    pMap.put("description", description);
                    pMap.put("imageUrl", imageUrl);
                    pMap.put("productUrl", productUrl);
                    pMap.put("price", price);
                    pMap.put("currency", currency);
                    pMap.put("availability", availability);
                    pMap.put("provider", provider);
                    pMap.put("timeStamp", timeStamp);
                    Util.productMap = (HashMap<String, String>) pMap;
                } else {
                    tuple = new StringBuilder((count++) + "").append(";").append(name).append(";").append(sku).append(";").append(description)
                            .append(";").append(imageUrl).append(";").append(productUrl)
                            .append(";").append(price).append(";").append(currency)
                            .append(";").append(availability)
                            .append(";").append(provider).append(";").append(timeStamp)
                            .toString().trim();

                    if (tuple.replaceAll(";", "").trim().isEmpty())
                        tuple = "";
                    else {
                        tuple = Util.clearLine(tuple);
                        tuple = Util.deCodeLine(tuple);
                    }
                }
                return tuple;
            }

            private String getProvider(String line) {
                String provider = "";
                try {
                    String[] details = line.split(">, ")[0].split("::");
                    provider = details[0] + details[1];
                } catch (Exception e) {
                    //Nothing is required; this is necessary for normal execution if line is empty/incomplete
                }
                return provider;
            }

            private String getTimeStamp(String line) {
                String timeStamp = "";
                try {
                    String[] details = line.split(">, ")[0].split("::");
                    timeStamp = details[2];
                } catch (Exception e) {
                    //Nothing is required; this is necessary for normal execution if line is empty/incomplete
                }
                return timeStamp;
            }

        }).filter(new Function<String, Boolean>() {

            //@Override
            public Boolean call(String arg0) throws Exception {
                return !arg0.isEmpty();
            }
        });

        entity.saveAsTextFile(args[1]);
    }

    public static boolean inList(String inputStr, String[] items) {
        for (int i = 0; i < items.length; i++) {
            if (inputStr.contains(items[i])) {
                return true;
            }
        }
        return false;
    }

}