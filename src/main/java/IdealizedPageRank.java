import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.mapred.Pair;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

public final class IdealizedPageRank {
    //groups with () are useful, i love them
    private static final Pattern links = Pattern.compile("^(\\d+):(.*)$");
    //ill figure out titles later i dont know what I'm looking at
    //private static final Pattern titles

    public static void main(String[] args) {
        String input = args[0];
        String output = args[1];

        Matcher linkMatcher = links.matcher(input);

        HashMap<String, String[]> linkMap = new HashMap<>();
        //look this isnt the right implementation in the slightest but its a placeholder to show how the regex works
        while (linkMatcher.find()) {
            linkMap.put(linkMatcher.group(1), linkMatcher.group(2).split(" "));
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaPageRank").master("local")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        PairFunction<String, String, String> linkPair =
                new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] tmp = s.split(":");
                        return new Tuple2<>(tmp[0], tmp[1]);
                    }
                };

        JavaPairRDD<String, String> linksRDD = lines.mapToPair(linkPair);

        System.out.println("Links:");
        linksRDD.take(4).forEach(System.out::println);
        
        JavaPairRDD<String, Double> ranks = linksRDD.mapValues(v -> {
            String[] tmp = v.split(" ");
            return 1.0 / tmp.length;
        });

        System.out.println("Rank:");
        linksRDD.take(4).forEach(System.out::println);

        }
}
