package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.slf4j.LoggerFactory
import java.util.Date


object Kudu2Hudi {

  private val log = LoggerFactory.getLogger("Kudu2Hudi")

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)
    Logger.getLogger("org").setLevel(Level.WARN)

    val parmas = Config.parseConfig(Kudu2Hudi, args)
    val spark = SparkHelper.getSparkSession(parmas.env)

    // get all tables in the database to migrate.
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

        val tableType = if (partitionKey != null && partitionKey.length > 0) "MERGE_ON_READ" else "COPY_ON_WRITE"

        writeHudiTable(df, parmas.syncDB, tableName, "bulk_insert", parmas.zookeeperUrl,
                          primaryKey, "created_ts", partitionKey, parmas.hudiBasePath, tableType)

        //could try to use multiple threads.
        //val t1 = new Thread(new Task(spark, parmas, tableName, primaryKey, partitionKey))
        //t1.start()
      })
    }
  }
}

object Task {
  private val log = LoggerFactory.getLogger("Kudu2Hudi")
}

class Task(val spark: SparkSession, val parmas: Config,
           val tableName: String, val primaryKey: String, val partitionKey: String) extends Runnable {
  override def run(): Unit = {
    val beginTime = (new Date).getTime

    val df = spark.read
      .option("kudu.master", parmas.kuduMaster)
      .option("kudu.table", "impala::" + parmas.kuduDatabase + "." + tableName)
      .format("kudu").load
      .withColumn("created_ts", lit((new Date()).getTime))

    val tableType = if (partitionKey != null && partitionKey.length > 0) "MERGE_ON_READ" else "COPY_ON_WRITE"

    writeHudiTable(df, parmas.syncDB, tableName, "insert", parmas.zookeeperUrl,
      primaryKey, "created_ts", partitionKey, parmas.hudiBasePath, tableType)

    val endTime = (new Date).getTime
    Task.log.info("*********** Thread := " + Thread.currentThread + "***** use time := " + (endTime - beginTime))
  }
}

//spark-submit --master yarn \
//  --deploy-mode cluster \
//  --driver-memory 24g \
//  --driver-cores 8 \
//  --executor-memory 16g \
//  --executor-cores 4 \
//  --num-executors 10 \
//  --packages org.apache.hudi:hudi-spark-bundle_2.11:0.10.0 \
//  --jars s3://dalei-demo/jars/kudu-client-1.10.0-cdh6.3.2.jar,s3://dalei-demo/jars/kudu-spark2_2.11-1.10.0-cdh6.3.2.jar,s3://dalei-demo/jars/ImpalaJDBC41.jar,s3://dalei-demo/jars/scopt_2.11-3.7.1.jar \
//  --class com.aws.analytics.Kudu2Hudi s3://dalei-demo/jars/emr5-hudi-example-1.0-SNAPSHOT.jar \
//  -e prod \
//  -i 100 -g s3://dalei-demo/hudi -z 1 -m 1 \
//  -s kudu_migration_full \
//  -l ip-10-0-0-85.ec2.internal \
//  -i jdbc:impala://10.0.0.16:21050/ \
//  -m cdh-master-1:7051,cdh-master-2:7051,cdh-master-3:7051 \
//  -d tpcds_data10g_kudu