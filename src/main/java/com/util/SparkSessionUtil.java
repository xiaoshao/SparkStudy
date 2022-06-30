package com.util;

import org.apache.spark.sql.SparkSession;

public class SparkSessionUtil {

    public static SparkSession createLocalSparkSession(String applicationName) {
        return SparkSession.builder().appName(applicationName).master("local[*]").getOrCreate();
    }
}
