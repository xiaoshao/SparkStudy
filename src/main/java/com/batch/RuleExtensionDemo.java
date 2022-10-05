package com.batch;

import com.util.DataUtil;
import com.util.SparkSessionUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RuleExtensionDemo {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSessionUtil.createSparkSessionWithMyExtension("extension");

        Dataset<Row> dataset = DataUtil.createDataset(sparkSession);

        dataset.registerTempTable("temp_table");

        Dataset<Row> rowDataset = sparkSession.sql("select 0 + first, first from temp_table");

        rowDataset.show();
    }
}
