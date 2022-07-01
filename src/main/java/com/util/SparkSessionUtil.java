package com.util;

import com.spark.extension.parser.MyParser;
import com.spark.extension.parser.MyParserBuilder;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;

public class SparkSessionUtil {

    public static SparkSession createLocalSparkSession(String applicationName) {
        return SparkSession.builder().appName(applicationName).master("local[*]").getOrCreate();
    }

    public static SparkSession createSparkSessionWithMyParser(String applicationName) {


        return SparkSession.builder().appName(applicationName).master("local[*]")
                .withExtensions(MyParserBuilder.extensionBuilder())
                .getOrCreate();
    }
}
