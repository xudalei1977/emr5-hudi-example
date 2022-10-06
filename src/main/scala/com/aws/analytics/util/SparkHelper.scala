package com.aws.analytics.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkHelper {


  def getSparkSession(env: String) = {

    env match {
      case "prod" => {
        val conf = new SparkConf()
          .setAppName("emr5-hudi-example")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.sql.hive.convertMetastoreParquet", "false")
          .set("spark.sql.debug.maxToStringFields", "500")
          .set("spark.sql.cbo.enabled", "true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable","true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy","NEVER")

        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()

      }

      case "dev" => {
        val conf = new SparkConf()
          .setAppName("emr5-hudi-example")
          .setMaster("local[6]")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.sql.hive.convertMetastoreParquet", "false")
          .set("spark.debug.maxToStringFields", "500")
          .set("spark.sql.cbo.enabled", "true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable","true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy","NEVER")
        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
      }
      case _ => {
        println("not match env, exits")
        System.exit(-1)
        null

      }
    }
  }

}
