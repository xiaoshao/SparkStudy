package com.util;

import com.spark.extension.parser.MyParserBuilder;
import org.apache.spark.sql.SparkSession;

public class SparkSessionUtil {

    public static SparkSession createLocalSparkSession(String applicationName) {
        return SparkSession.builder().appName(applicationName).master("local[*]").getOrCreate();
    }

    public static SparkSession createSparkSessionWithMyExtension(String applicationName) {
        return SparkSession.builder().appName(applicationName).master("local[*]")
                .withExtensions(MyParserBuilder.extensionBuilder())
                .getOrCreate();
    }

    public static SparkSession localStandaloneSparkSession(String applicationName) {
        return SparkSession.builder().appName(applicationName)
                .master("spark://localhost:7077")
                .config("spark.executor.memory", "1G")
                .config("spark.executor.cores", "1")
                .config("spark.executor.instances", "1")
                .getOrCreate();
    }
}
