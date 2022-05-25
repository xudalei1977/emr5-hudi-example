package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory


object Hive2Hudi {

  private val log = LoggerFactory.getLogger("Hive2Hudi")

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)

    // Set log4j level to warn
    Logger.getLogger("org").setLevel(Level.WARN)

    //System.setProperty("HADOOP_USER_NAME", "hadoop")
    val parmas = Config.parseConfig(Hive2Hudi, args)

    // init spark session
    val spark = SparkHelper.getSparkSession(parmas.env)
    val df = spark.read.format("parquet")
      .load(s"${parmas.hudiBasePath}/${parmas.syncDB}/${parmas.syncTableName}")
      .filter(!_.anyNull)
      .repartition(5000)

    writeHudiTable(df, parmas.syncDB, parmas.syncTableName, parmas.hudiWriteOperation, parmas.zookeeperUrl,
      parmas.hudiKeyField, parmas.hudiCombineField, parmas.hudiPartition, parmas.hudiBasePath, parmas.tableType)
  }
}

//val df = spark.read.format("parquet").
//load(s"s3://dalei-demo/tpcds/data10g/call_center").
//writeHudiTable(df, "kudu_migration", "call_center2", "insert", "ip-10-0-0-113.ec2.internal",
//"cc_call_center_sk", "", "", "s3://dalei-demo/hudi")

//spark-shell --master yarn \
//--deploy-mode client \
//--driver-memory 16g \
//--driver-cores 4 \
//--executor-memory 16g \
//--executor-cores 4 \
//--num-executors  10 \
//--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
//--conf "spark.sql.hive.convertMetastoreParquet=false" \
//--packages org.apache.hudi:hudi-spark-bundle_2.11:0.10.0 \
//--jars s3://dalei-demo/jars/kudu-client-1.10.0-cdh6.3.2.jar,s3://dalei-demo/jars/kudu-spark2_2.11-1.10.0-cdh6.3.2.jar,s3://dalei-demo/jars/ImpalaJDBC41.jar,s3://dalei-demo/jars/scopt_2.11-3.7.1.jar \
//  --class com.aws.analytics.Hive2Hudi s3://dalei-demo/jars/emr-hudi-example-1.0-SNAPSHOT.jar \
//  -e dev \
//  -g s3://dalei-demo/hudi -y COW \
//  -s tpcds_data10g_hudi -u household_demographics \
//  -z hd_demo_sk -q ws_sold_date_sk \
//  -l ip-10-0-0-124.ec2.internal
