/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 *
 * Example Usage:
 * <pre>
 * bin/run-example JavaPageRank data/mllib/pagerank_data.txt 10
 * </pre>
 */
public final class JavaPageRank {
  private static final Pattern SPACES = Pattern.compile("\\s+");

  static void showWarning() {
    String warning = "WARN: This is a naive implementation of PageRank " +
            "and is given as an example! \n" +
            "Please use the PageRank implementation found in " +
            "org.apache.spark.graphx.lib.PageRank for more conventional use.";
    System.err.println(warning);
  }

  private static class Sum implements Function2<Double, Double, Double> {
    @Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }

  public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount").master("local")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaPairRDD<String, String> links = lines.mapToPair(s -> new Tuple2<>(s.split(": ")[0], s.split(": ")[1]));
        long nPage = links.count();
        JavaPairRDD<String, Double>  ranks = links.mapValues(v1 -> 1.0/(double)nPage);


        for (int i = 0; i < 25; i++) {
            JavaPairRDD<String, Double> con = links.join(ranks).values().flatMapToPair(s -> {
                String[] outgoingl = s._1().split(" ");
                int len = outgoingl.length;
                List<Tuple2<String, Double>> results = new ArrayList<>();
                for (String n : outgoingl) {
                    results.add(new Tuple2<>(n, s._2() / len));
                }

                return results.iterator();


            });
            ranks = con.reduceByKey((i1, i2) -> i1 + i2);
        }



        List<Tuple2<String, Double>> output = ranks.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + "\t" + tuple._2());
        }
        spark.stop();
    }
}
