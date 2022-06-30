package com.batch;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class FirstDemo {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("test").master("local[*]").getOrCreate();

        StructType schema = new StructType(new StructField[]{
                new StructField("first", DataTypes.StringType, true, Metadata.empty())
        });
        ss.read().format("csv").schema(schema).load("input/batch/FirstDemo").write().format("json").save("output/batch/FirstDemo");
    }
}
