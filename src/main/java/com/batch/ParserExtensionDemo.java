package com.batch;

import com.util.DataUtil;
import com.util.SparkSessionUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParserExtensionDemo {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSessionUtil.createSparkSessionWithMyExtension("testApp");

        Dataset<Row> dataset = DataUtil.createDataset(sparkSession);

        dataset.registerTempTable("temp_table");

        sparkSession.sql("select * from temp_table").write().format("console").save();
    }
}
