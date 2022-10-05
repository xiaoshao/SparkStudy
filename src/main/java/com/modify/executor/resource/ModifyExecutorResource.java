package com.modify.executor.resource;

import com.util.SparkSessionUtil;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.resource.ExecutorResourceRequest;
import org.apache.spark.resource.ResourceProfile;
import org.apache.spark.resource.ResourceProfileManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

import java.lang.reflect.Field;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class ModifyExecutorResource {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession sparkSession = SparkSessionUtil.localStandaloneSparkSession("localStandalone");

        SparkConf conf = sparkSession.sparkContext().getConf();
        StructType schema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, true, Metadata.fromJson("{}")),
                new StructField("age", DataTypes.TimestampType, true, Metadata.fromJson("{}")),
        });
        Dataset<Row> csv = sparkSession
                .readStream()
                .format("csv")
                .schema(schema)
                .load("/srv/input");

        csv.writeStream()
                .format("csv")
                .option("checkpointLocation", "/srv/checkpoint")
                .option("path", "/srv/output")
                .outputMode("append")
                .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
                .start();

        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(10, new DefaultThreadFactory("test"));

        scheduledThreadPoolExecutor.scheduleAtFixedRate(new RequestExecutorRunnable(sparkSession), 10, 60, TimeUnit.SECONDS);

        sparkSession.streams().awaitAnyTermination();
    }

    public static class RequestExecutorRunnable implements Runnable {

        private SparkSession sparkSession;

        private SparkContext sc;

        public static AtomicInteger count = new AtomicInteger(0);

        public RequestExecutorRunnable(SparkSession sparkSession) {
            this.sparkSession = sparkSession;
            this.sc = sparkSession.sparkContext();
        }

        @Override
        public void run() {
            Seq<String> executorIds = sc.getExecutorIds();

            if (count.incrementAndGet() > 2) {
                System.out.println(sc.getConf().get("spark.executor.memory"));
                System.out.println(sc.getConf().get("spark.executor.cores"));
            } else {
                SparkConf conf = sc.getConf();
                conf.set("spark.executor.memory", "2G", true);
                conf.set("spark.executor.cores", "2", true);
                try {
                    Field conf1Field = SparkContext.class.getDeclaredField("_conf");
                    Field conf1Field1 = ResourceProfileManager.class.getDeclaredField("sparkConf");
                    Field defaultProfile = ResourceProfileManager.class.getDeclaredField("defaultProfile");
                    conf1Field.setAccessible(true);
                    conf1Field1.setAccessible(true);
                    conf1Field.set(sc, conf);
                    conf1Field1.set(sc.resourceProfileManager(), conf);
                    defaultProfile.setAccessible(true);
                    Object o = defaultProfile.get(sc.resourceProfileManager());

                    System.out.println("test");
                    ExecutorResourceRequest cores = ((ResourceProfile) o).executorResources().get("cores").get();

                    Field amountField = ExecutorResourceRequest.class.getDeclaredField("amount");
                    amountField.setAccessible(true);
                    amountField.set(cores, 4L);
                } catch (NoSuchFieldException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }

                System.out.println(executorIds.seq().size());

                sc.killExecutors(executorIds.seq());

                sc.requestExecutors(2);
            }

        }
    }
}
