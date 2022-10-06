[toc]

### EMR Hudi Example

#### 1. 程序

1. Kudu2Hudi: Spark读取Kudu表数据，将数据写入Hudi表，Schema同步到Hive
2. Kudu2MSK: 读取Kudu数据，写入MSK
3. Kudu2Parquet: 将Kudu表的数据导出到Parquet文件，可以上传到S3
4. MSK2Hudu: 读取MSK里到数据，写入Hudi表，可以读取多个Topic，一个Topic对应到一张Hudi表

#### 2. 环境 

```markdown
*  EMR 5.35.0 (spark 2.4.8 hudi 0.10.0)
```

#### 3. 编译 & 运行

##### 3.1 编译

```properties
# 编译 mvn clean package -Dscope.type=provided 
```


##### 3.3 运行程序说明

* 支持参数

```properties
  Log2Hudi 1.0
  Usage: spark ss Log2Hudi [options]
  
    -e, --env <value>        env: dev or prod
    -b, --brokerList <value>
                             kafka broker list,sep comma
    -t, --sourceTopic <value>
                             kafka topic
    -p, --consumeGroup <value>
                             kafka consumer group
    -j, --jsonMetaSample <value>
                             json meta sample
    -s, --syncHive <value>   whether sync hive，default:false
    -o, --startPos <value>   kafka start pos latest or earliest,default latest
    -i, --trigger <value>    default 300 second,streaming trigger interval
    -c, --checkpointDir <value>
                             hdfs dir which used to save checkpoint
    -g, --hudiEventBasePath <value>
                             hudi event table hdfs base path
    -s, --syncDB <value>     hudi sync hive db
    -u, --syncTableName <value>
                             hudi sync hive table name
    -y, --tableType <value>  hudi table type MOR or COW. default COW
    -r, --syncJDBCUrl <value>
                             hive server2 jdbc, eg. jdbc:hive2://172.17.106.165:10000
    -n, --syncJDBCUsername <value>
                             hive server2 jdbc username, default: hive
    -p, --partitionNum <value>
                             repartition num,default 16
    -t, --morCompact <value>
                             mor inline compact,default:true
    -m, --inlineMax <value>  inline max compact,default:20
    -w, --hudiWriteOperation <value>
                             hudi write operation,default insert
    -z, --hudiKeyField <value>
                             hudi key field, recordkey,precombine
    -q, --hudiPartition <value>
                             hudi partition column,default logday,hm
```

* 启动样例

```shell
  # 1. 下面最有一个 -j 参数，是告诉程序你的生成的json数据是什么样子，只要将生成的json数据粘贴一条过来即可，程序会解析这个json作为schema。 
  # 2. --jars 注意替换为你需要的hudi版本
  # 3. --class 参数注意替换你自己的编译好的包
  # 4. -b 参数注意替换为你自己的kafka地址
  # 5. -t 替换为你自己的kafka topic
  # 6. -i 参数表示streming的trigger interval，10表示10秒
  # 7. -w 参数配置 upsert，还是insert。
  # 8. -z 分别表示hudi的recordkey,precombine 字段是什么
  # 9. -q 表示hudi分区字段，程序中会自动添加logday当前日期，hm字段是每小时10分钟。相当于按天一级分区，10分钟二级分区，eg. logday=20210624/hm=0150 . 也可以选择配置其他字段。
  # 10. 其他参数按照支持的参数说明修改替换
  spark-submit --master yarn \
  --deploy-mode cluster \
  --driver-memory 24g \
  --driver-cores 8 \
  --executor-memory 16g \
  --executor-cores 4 \
  --num-executors 10 \
  --packages org.apache.hudi:hudi-spark-bundle_2.11:0.10.0 \
  --jars s3://dalei-demo/jars/kudu-client-1.10.0-cdh6.3.2.jar,s3://dalei-demo/jars/kudu-spark2_2.11-1.10.0-cdh6.3.2.jar,s3://dalei-demo/jars/ImpalaJDBC41.jar,s3://dalei-demo/jars/scopt_2.11-3.7.1.jar \
  --class com.aws.analytics.Kudu2Hudi s3://dalei-demo/jars/emr5-hudi-example-1.0-SNAPSHOT.jar \
  -e prod \
  -i 100 -g s3://dalei-demo/hudi -z 1 -m 1 \
  -s kudu_migration_full \
  -l ip-10-0-0-85.ec2.internal \
  -i jdbc:impala://10.0.0.16:21050/ \
  -m cdh-master-1:7051,cdh-master-2:7051,cdh-master-3:7051 \
  -d tpcds_data10g_kudu
```

