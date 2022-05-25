package com.aws.analytics.conf

case class Config(
                   env: String = "",
                   brokerList: String = "",
                   sourceTopic: String = "",
                   consumerGroup: String = "",
                   checkpointDir: String ="",
                   path:String = "",
                   tableInfoJson: String ="",
                   trigger:String = "300",
                   hudiBasePath:String ="",
                   tableType:String="COW",
                   syncDB:String ="",
                   syncTableName:String ="",
                   syncJDBCUrl:String ="",
                   syncJDBCUsername:String ="hive" ,
                   jsonMetaSample:String ="",
                   syncHive:String="false",
                   partitionNum:String ="16",
                   partitionFormat:String ="yyyy-MM-dd-HH-mm",
                   startPos:String="latest",
                   morCompact:String="true",
                   inlineMax:String="20",
                   hudiWriteOperation:String = "insert" ,
                   hudiKeyField:String = "",
                   hudiCombineField:String = "",
                   hudiPartition:String = "",
                   concurrent:String = "false",
                   zookeeperUrl:String = "",
                   zookeeperPort:String = "2181",
                   hudiAggBasePath:String = "",
                   impalaJdbcUrl:String = "",
                   kuduMaster:String = "",
                   kuduDatabase:String = "",
                   filterString:String = ""
                 )

object Config {

  def parseConfig(obj: Object,args: Array[String]): Config = {
    val programName = obj.getClass.getSimpleName.replaceAll("\\$","")
    val parser = new scopt.OptionParser[Config]("spark ss "+programName) {
      head(programName, "1.0")
      opt[String]('e', "env").required().action((x, config) => config.copy(env = x)).text("env: dev or prod")
      opt[String]('b', "brokerList").optional().action((x, config) => config.copy(brokerList = x)).text("kafka broker list,sep comma")
      opt[String]('t', "sourceTopic").optional().action((x, config) => config.copy(sourceTopic = x)).text("kafka topic")
      opt[String]('p', "consumerGroup").optional().action((x, config) => config.copy(consumerGroup = x)).text("kafka consumer group")
      opt[String]('o', "startPos").optional().action((x, config) => config.copy(startPos = x)).text("kafka start pos latest or earliest,default latest")
      opt[String]('i', "trigger").optional().action((x, config) => config.copy(trigger = x)).text("default 300 second,streaming trigger interval")
      opt[String]('c', "checkpointDir").optional().action((x, config) => config.copy(checkpointDir = x)).text("hdfs dir which used to save checkpoint")
      opt[String]('g', "hudiBasePath").optional().action((x, config) => config.copy(hudiBasePath = x)).text("hudi event table hdfs base path")
      opt[String]('y', "tableType").optional().action((x, config) => config.copy(tableType = x)).text("hudi table type MOR or COW. default COW")
      opt[String]('s', "syncDB").optional().action((x, config) => config.copy(syncDB = x)).text("hudi sync hive db")
      opt[String]('u', "syncTableName").optional().action((x, config) => config.copy(syncTableName = x)).text("hudi sync hive table name")
      opt[String]('w', "hudiWriteOperation").optional().action((x, config) => config.copy(hudiWriteOperation = x)).text("hudi write operation,default insert")
      opt[String]('z', "hudiKeyField").optional().action((x, config) => config.copy(hudiKeyField = x)).text("hudi key field, recordkey")
      opt[String]('m', "hudiCombineField").optional().action((x, config) => config.copy(hudiCombineField = x)).text("hudi combine field, precombine")
      opt[String]('q', "hudiPartition").optional().action((x, config) => config.copy(hudiPartition = x)).text("hudi partition column,default logday,hm")

      programName match {
        case "MSK2Hudi" =>
          opt[String]('t', "morCompact").optional().action((x, config) => config.copy(morCompact = x)).text("mor inline compact,default:true")
          opt[String]('m', "inlineMax").optional().action((x, config) => config.copy(inlineMax = x)).text("inline max compact,default:20")
          opt[String]('l', "zookeeperUrl").optional().action((x, config) => config.copy(zookeeperUrl = x)).text("zookeeper url for hudi")
          opt[String]('t', "zookeeperPort").optional().action((x, config) => config.copy(zookeeperPort = x)).text("zookeeper port for hudi")
          opt[String]('i', "impalaJdbcUrl").required().action((x, config) => config.copy(impalaJdbcUrl = x)).text("impalaJdbc url for kudu")
          opt[String]('d', "kuduDatabase").required().action((x, config) => config.copy(kuduDatabase = x)).text("kudu database will be migrated")

        case "Aggregation" =>
          opt[String]('a', "hudiAggBasePath").optional().action((x, config) => config.copy(hudiAggBasePath = x)).text("hudi aggregation table hdfs base path")

        case "Hive2Hudi" =>
          opt[String]('l', "zookeeperUrl").optional().action((x, config) => config.copy(zookeeperUrl = x)).text("zookeeper url for hudi")

        case "Kudu2Hudi" =>
          opt[String]('l', "zookeeperUrl").optional().action((x, config) => config.copy(zookeeperUrl = x)).text("zookeeper url for hudi")
          opt[String]('i', "impalaJdbcUrl").required().action((x, config) => config.copy(impalaJdbcUrl = x)).text("impalaJdbc url for kudu")
          opt[String]('m', "kuduMaster").required().action((x, config) => config.copy(kuduMaster = x)).text("kudu master and port")
          opt[String]('d', "kuduDatabase").required().action((x, config) => config.copy(kuduDatabase = x)).text("kudu database will be migrated")

        case "Kudu2Parquet" =>
          opt[String]('l', "zookeeperUrl").optional().action((x, config) => config.copy(zookeeperUrl = x)).text("zookeeper url for hudi")
          opt[String]('i', "impalaJdbcUrl").required().action((x, config) => config.copy(impalaJdbcUrl = x)).text("impalaJdbc url for kudu")
          opt[String]('m', "kuduMaster").required().action((x, config) => config.copy(kuduMaster = x)).text("kudu master and port")
          opt[String]('d', "kuduDatabase").required().action((x, config) => config.copy(kuduDatabase = x)).text("kudu database will be migrated")

        case "Kudu2MSK" =>
          opt[String]('l', "zookeeperUrl").optional().action((x, config) => config.copy(zookeeperUrl = x)).text("zookeeper url for hudi")
          opt[String]('i', "impalaJdbcUrl").required().action((x, config) => config.copy(impalaJdbcUrl = x)).text("impalaJdbc url for kudu")
          opt[String]('m', "kuduMaster").required().action((x, config) => config.copy(kuduMaster = x)).text("kudu master and port")
          opt[String]('d', "kuduDatabase").required().action((x, config) => config.copy(kuduDatabase = x)).text("kudu database will be migrated")
          opt[String]('f', "filterString").optional().action((x, config) => config.copy(filterString = x)).text("kudu table filter string")
      }

    }
    parser.parse(args, Config()) match {
      case Some(conf) => conf
      case None => {
        System.exit(-1)
        null
      }
    }

  }

}

