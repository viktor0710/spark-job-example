/**
  * Created by p3700698 on 11/1/16.
  */
package com.ness

import org.apache.spark.sql.SparkSession

object Main {

    def main( args: Array[String]) : Unit = {
      val spark: SparkSession = SparkSession.builder
        .appName("Test app")
        .master("yarn")
        .config("spark.submit.deployMode", "client")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", 3)
        .config("spark.default.parallelism", 12)
        .config("spark.sql.shuffle.partitions", 12)
        .getOrCreate

      if (args(0).equals("count")){
        print ("###" + spark.sparkContext.parallelize(Array(1,2,3,4,5,6)).count())
      } else {
        if (args(0).equals("sum")) {
          print("###" + spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6)).sum())
        } else {
          print("###noop")
        }
      }
    }
}
