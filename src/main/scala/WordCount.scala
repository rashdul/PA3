import org.apache.spark.sql.SparkSession


object WordCount {

  def main(args: Array[String]): Unit = {

    // uncomment below line and change the placeholders accordingly
//    val sc = SparkSession.builder().master("spark://<SPARK_MASTER_IP>:<SPARK_MASTER_PORT>").getOrCreate().sparkContext

    // to run locally in IDE,
    // But comment out when creating the jar to run on cluster
    val sc = SparkSession.builder().master("local").getOrCreate().sparkContext

    // to run with yarn, but this will be quite slow, if you like try it too
    // when running on the cluster make sure to use "--master yarn" option
//    val sc = SparkSession.builder().master("yarn").getOrCreate().sparkContext

    val text = sc.textFile(args(0))
    val counts = text.flatMap(line => line.split(" ")
    ).map(word => (word,1)).reduceByKey(_+_)
    counts.saveAsTextFile(args(1))
  }
}
