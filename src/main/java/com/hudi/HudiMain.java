package com.hudi;

import com.util.SparkSessionUtil;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class HudiMain {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSessionUtil.createLocalSparkSession("test");

        List<String> datas = new ArrayList<>();
        datas.add("1,12,hello");

//        sparkSession.createDataset(datas, );
    }
}
