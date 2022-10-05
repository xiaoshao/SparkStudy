package com.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class DataUtil {
    public static Dataset<Row> createDataset(SparkSession sparkSession) {

        List<String> data = new ArrayList<>();

        data.add("1,s1,2021-10-1 10:10:10");
        data.add("2,s2,2021-10-1 10:10:11");
        data.add("3,s3,2021-10-1 10:10:11");
        Dataset<String> dataset = sparkSession.createDataset(data, Encoders.STRING());

        StructType schema = new StructType(new StructField[]{
                new StructField("first", DataTypes.IntegerType, true, Metadata.fromJson("{}")),
                new StructField("second", DataTypes.StringType, true, Metadata.fromJson("{}")),
                new StructField("timeId", DataTypes.TimestampType, true, Metadata.fromJson("{}"))
        });

        return sparkSession.read().schema(schema).csv(dataset).toDF();
    }
}
