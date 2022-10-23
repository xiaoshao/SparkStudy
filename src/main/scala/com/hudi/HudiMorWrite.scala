package com.hudi

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{COW_TABLE_TYPE_OPT_VAL, MOR_TABLE_TYPE_OPT_VAL, PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object HudiMorWrite {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local[4]")
    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val schema = StructType(
      Seq(
        StructField("id", DataTypes.IntegerType, true, Metadata.fromJson("{}")),
        StructField("name", DataTypes.StringType, true, Metadata.fromJson("{}")),
        StructField("price", DataTypes.DoubleType, true, Metadata.fromJson("{}")),
        StructField("ts", DataTypes.TimestampType, true, Metadata.fromJson("{}")),
        StructField("partition", DataTypes.StringType, true, Metadata.fromJson("{}"))
      ))

    val input = spark.read.schema(schema).csv("input/batch/hudi")

    val hudiTableName: String = "mor1"
    val hudiTablePath: String = "/Users/shaozengwei/projects/SparkStudy/output/hudi/" + hudiTableName

    input.write.mode(SaveMode.Overwrite)
      .format("hudi")
      .option(TABLE_TYPE.key(), MOR_TABLE_TYPE_OPT_VAL)
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "timestamp")
      .option(PARTITIONPATH_FIELD.key(), "partition")
      .option("hoodie.table.name", hudiTableName)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .save(hudiTablePath)

    spark.read.format("hudi").load(hudiTablePath).show()
    spark.stop()
  }

}
