package com.ScalaHbase

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession


object HbaseTest {
  case class EmpRow(empID:String, name:String, city:String)
  def parseRow(result:Result):EmpRow ={
    val rowkey = Bytes.toString(result.getRow())
    val cfDataBytes = Bytes.toBytes("inf")

    val d0 = rowkey
    val d1 = Bytes.toString(result.getValue(cfDataBytes, Bytes.toBytes("name")))
    val d2 = Bytes.toString(result.getValue(cfDataBytes, Bytes.toBytes("city")))
    EmpRow(d0,d1,d2)
  }
  def getHbaseConnection(conf: Config, env: String): Connection = {
    //Create Hbase Configuration Object
    val hbaseConfig: Configuration = HBaseConfiguration.create()
    hbaseConfig.set("hbase.zookeeper.quorum",
      conf.getString("zookeeper.quorum"))
    hbaseConfig.set("hbase.zookeeper.property.clientPort",
      conf.getString("zookeeper.port"))
    if (env != "dev") {
      hbaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure")
      hbaseConfig.set("hbase.cluster.distributed", "true")
    }
    val connection = ConnectionFactory.createConnection(hbaseConfig)
    connection
  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Consumer").getOrCreate()
    val env = args(0)
    val conf = ConfigFactory.load.getConfig(env)
    val hconf = getHbaseConnection(conf,env)
    hconf.getConfiguration.set(TableInputFormat.INPUT_TABLE,"empLansrod")
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(hconf.getConfiguration, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    import spark.implicits._
    val resultRDD = hbaseRDD.map(tuple => tuple._2)
    val empRDD = resultRDD.map(parseRow)
    val empDF = empRDD.toDF

    empDF.show()

  }
}
