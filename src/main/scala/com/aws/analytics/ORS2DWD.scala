package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.{HudiConfig, JsonSchema, Meta, SparkHelper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.slf4j.LoggerFactory
import org.apache.hudi.DataSourceReadOptions._
import java.time.LocalDateTime
import java.util.Date
import java.text.SimpleDateFormat


object ORS2DWD {

  private val log = LoggerFactory.getLogger("aggregation")

  private val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmssSSS")
  private val INTERVAL = 60000

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)

    // Set log4j level to warn
    Logger.getLogger("org").setLevel(Level.WARN)

    //System.setProperty("HADOOP_USER_NAME", "hadoop")
    val parmas = Config.parseConfig(ORS2DWD, args)

    // init spark session
    val ss = SparkHelper.getSparkSession(parmas.env)

    // init the aggregation table
    var beginTime: Date = new Date()
    var endTime: Date = null

    //process the initial hudi
    ss.read.format("hudi")
      .load(parmas.hudiBasePath)
      .createOrReplaceTempView("init_detail")

    ss.sql("select cardDate, count(id) as order_sum, sum(cast(money as double)) as money_sum " +
      "from init_detail group by cardDate")
      .createOrReplaceTempView("init_agg")

    val initAggDF = ss.sql("select cardDate, status, cast(unix_timestamp() as string) as ts, order_sum, money_sum from init_agg").toDF()
    write2HudiFromDF(initAggDF, parmas)

    while(true){
      Thread.sleep(INTERVAL)

      endTime = DATE_FORMAT.parse(DATE_FORMAT.format(beginTime.getTime + INTERVAL))

      //get the dataframe of hudi within
      ss.read.format("hudi")
                    .option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL)
                    .option(BEGIN_INSTANTTIME.key(), DATE_FORMAT.format(beginTime))
                    .option(END_INSTANTTIME.key(), DATE_FORMAT.format(endTime))
                    .load(parmas.hudiBasePath)
                    .createOrReplaceTempView("incre_detail")

      ss.sql("select cardDate, status, count(id) as order_sum, sum(cast(money as double)) as money_sum from incre_detail group by cardDate, status")
        .createOrReplaceTempView("incre_agg")

      ss.read.format("hudi")
                      .load(parmas.hudiAggBasePath)
                      .createOrReplaceTempView("taxi_order_agg")

      val increAggDF = ss.sql(
        """select i.cardDate, i.status, cast(unix_timestamp() as string) as ts,
          | (i.order_sum + s.order_sum) as order_sum, (i.money_sum + s.money_sum) as money_sum
          | from incre_agg i join taxi_order_agg s on i.cardDate = s.cardDate and i.status = s.status""".stripMargin)

      //save the hudi table
      write2HudiFromDF(increAggDF, parmas)

      beginTime = endTime
    }

  }

  def write2HudiFromDF(batchDF: DataFrame, parmas: Config) = {
    val newsDF = batchDF.filter(_ != null)
    if (!newsDF.isEmpty) {
      newsDF.persist()
      //  newsDF.show()

      println(LocalDateTime.now() + " === start writing table")
      newsDF.write.format("hudi").
        option("hoodie.datasource.write.table.type", "COPY_ON_WRITE").
        option("hoodie.table.name", "taxi_order_agg").
        option("hoodie.datasource.write.operation", "upsert").
        option("hoodie.datasource.write.recordkey.field", "cardDate,status").
        option("hoodie.datasource.write.precombine.field", "ts").
        option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator").
        option("hoodie.clean.async", "true").
        option("hoodie.clean.automatic", "true").
        option("hoodie.cleaner.commits.retained", "2").
        option("hoodie.keep.min.commits", "3").
        option("hoodie.keep.max.commits", "4").
        option("hoodie.datasource.hive_sync.enable", "true").
        option("hoodie.datasource.hive_sync.mode", "hms").
        option("hoodie.datasource.hive_sync.database", "tesla").
        option("hoodie.datasource.hive_sync.table", "taxi_order_agg").
        option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.NonPartitionedExtractor").
        option("hoodie.datasource.write.payload.class","org.apache.hudi.common.model.DefaultHoodieRecordPayload").
      mode(SaveMode.Append).
        save(parmas.hudiAggBasePath)

      newsDF.unpersist()
      println(LocalDateTime.now() + " === finish")
    }
  }

}

