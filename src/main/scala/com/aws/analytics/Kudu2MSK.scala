package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.SparkHelper
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, struct, to_json}
import org.slf4j.LoggerFactory

import java.util.UUID
import java.text.SimpleDateFormat
import java.util.Date


object Kudu2MSK {

  private val log = LoggerFactory.getLogger("Kudu2MSK")

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)

    // Set log4j level to warn
    Logger.getLogger("org").setLevel(Level.WARN)

    //System.setProperty("HADOOP_USER_NAME", "hadoop")
    val parmas = Config.parseConfig(Kudu2MSK, args)

    // init spark session
    val spark = SparkHelper.getSparkSession(parmas.env)

    val df = spark.read
      .option("kudu.master", parmas.kuduMaster)
      .option("kudu.table", "impala::" + parmas.kuduDatabase + "." + parmas.syncTableName)
      .format("kudu").load
      .withColumn("created_ts", lit((new Date()).getTime))
      .filter(parmas.filterString)
      .select(to_json(struct("*"), Map("dropFieldIfAllNull" -> "false")).as("value"))
      .selectExpr(s"cast('${UUID.randomUUID().toString}' as string)", "cast(value as string)")

    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", parmas.brokerList)
      .option("topic", parmas.sourceTopic)
      .save()
  }
}



//spark-shell --master yarn \
//--deploy-mode client \
//--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
//--conf "spark.sql.hive.convertMetastoreParquet=false" \
//--jars s3://dalei-demo/jars/kudu-client-1.10.0-cdh6.3.2.jar,s3://dalei-demo/jars/kudu-spark2_2.11-1.10.0-cdh6.3.2.jar,s3://dalei-demo/jars/ImpalaJDBC41.jar
//
//spark-submit --master yarn \
//  --deploy-mode client \
//  --conf "spark.sql.jsonGenerator.ignoreNullFields=false" \
//  --jars s3://dalei-demo/jars/kudu-client-1.10.0-cdh6.3.2.jar,s3://dalei-demo/jars/kudu-spark2_2.11-1.10.0-cdh6.3.2.jar,s3://dalei-demo/jars/scopt_2.11-3.7.1.jar,/usr/lib/spark/external/lib/spark-sql-kafka-0-10.jar,/usr/lib/spark/external/lib/spark-streaming-kafka-0-10-assembly.jar,/usr/lib/hudi/cli/lib/kafka-clients-2.0.0.jar \
//  --class com.aws.analytics.Kudu2MSK s3://dalei-demo/jars/emr5-hudi-example-1.0-SNAPSHOT.jar \
//  -e dev \
//  -b b-3.tesla.4yy9yf.c5.kafka.us-east-1.amazonaws.com:9092,b-1.tesla.4yy9yf.c5.kafka.us-east-1.amazonaws.com:9092,b-2.tesla.4yy9yf.c5.kafka.us-east-1.amazonaws.com:9092 \
//  -t kudu.store_sales \
//  -i 10 -c /home/hadoop/checkpoint -m 20 -u store_sales \
//  -i jdbc:impala://10.0.0.16:21050/ \
//  -m cdh-master-1:7051,cdh-master-2:7051,cdh-master-3:7051 \
//  -d tpcds_data10g_kudu \
//  -f "ss_sold_date_sk = 2450816"