package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.SparkHelper
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import org.slf4j.LoggerFactory

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date


object Kudu2Parquet {

  private val log = LoggerFactory.getLogger("Kudu2Parquet")

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)
    Logger.getLogger("org").setLevel(Level.WARN)

    val parmas = Config.parseConfig(Kudu2Parquet, args)
    val spark = SparkHelper.getSparkSession(parmas.env)

    // get all table in the database to migrate.
    val allTable = queryByJdbc(parmas.impalaJdbcUrl + parmas.kuduDatabase, "show tables")

    if(allTable != null && allTable.isInstanceOf[Seq[String]] && allTable.length > 0) {
      allTable.foreach( tableName => {
        val (primaryKey, partitionKey) = getPrimaryAndPartitionKey(parmas.impalaJdbcUrl + parmas.kuduDatabase, tableName)

        val df = spark.read
          .option("kudu.master", parmas.kuduMaster)
          .option("kudu.table", "impala::" + parmas.kuduDatabase + "." + tableName)
          .format("kudu").load
          .filter(genPrimaryKeyFilter(primaryKey))
          .withColumn("created_ts", lit((new Date()).getTime))
          .repartition(parmas.partitionNum)

        df.write.format("parquet").mode(SaveMode.Append).save(s"/tmp/$tableName")
      })
    }
  }
}

//spark-submit --master yarn \
//  --deploy-mode client \
//  --packages org.apache.hudi:hudi-spark-bundle_2.11:0.10.0 \
//  --jars ./kudu-client-1.10.0-cdh6.3.2.jar,./kudu-spark2_2.11-1.10.0-cdh6.3.2.jar,./ImpalaJDBC41.jar,./scopt_2.11-3.7.1.jar \
//  --class com.aws.analytics.Kudu2Parquet ./emr5-hudi-example-1.0-SNAPSHOT.jar \
//  -e dev \
//  -i 100 -g s3://dalei-demo/hudi -z 1 -m 1 \
//  -s kudu_migration_full \
//  -l ip-10-0-0-85.ec2.internal \
//  -i jdbc:impala://10.0.0.16:21050/ \
//  -m cdh-master-1:7051,cdh-master-2:7051,cdh-master-3:7051 \
//  -d tpcds_data10g_kudu