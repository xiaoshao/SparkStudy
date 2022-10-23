package com.hudi;

import com.util.SparkSessionUtil;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

public class HudiMain {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSessionUtil.createLocalSparkSession("test");

        List<String> datas = new ArrayList<>();
        datas.add("1,'hello',300.0");

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.fromJson("{}")),
                new StructField("name", DataTypes.StringType, true, Metadata.fromJson("{}")),
                new StructField("price", DataTypes.DoubleType, true, Metadata.fromJson("{}")),
        });

        ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);
//        sparkSession.createDataset(datas, encoder);
    }
}
