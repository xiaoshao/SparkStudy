package com.stream;

import com.util.SparkSessionUtil;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.concurrent.TimeoutException;

public class StreamDemo {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        StructType schema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, true, Metadata.fromJson("{}")),
                new StructField("age", DataTypes.IntegerType, true, Metadata.fromJson("{}")),
                new StructField("time", DataTypes.TimestampType, true, Metadata.fromJson("{}")),
        });

        SparkSession sparkSession = SparkSessionUtil.createLocalSparkSession("local session");

        Dataset<Row> csv = sparkSession.readStream().format("csv").schema(schema).load("input/batch/FirstDemo");

        Dataset<Long> mapResult = csv.map(new MapFunction<Row, Long>() {
            @Override
            public Long call(Row value) throws Exception {
                Timestamp timestamp = value.getTimestamp(2);

                return Long.valueOf(timestamp.toString());
            }
        }, Encoders.LONG());

        mapResult.writeStream().format("Console").option("checkpointLocation", "output/meta").trigger(Trigger.ProcessingTime("1 minutes")).start();

        sparkSession.streams().awaitAnyTermination();
    }
}
