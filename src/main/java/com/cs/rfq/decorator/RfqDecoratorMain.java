package com.cs.rfq.decorator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class RfqDecoratorMain {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");

        //TODO: create a Spark configuration and set a sensible app name
        SparkConf conf = new SparkConf().setAppName("StreamFromFile"); ////////////////change to a sensible app name

        //TODO: create a Spark streaming context
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //TODO: create a Spark session
        SparkSession session = SparkSession.builder()
                .appName("RFQDecoratorMain") ////////change name to better name maybe idk
                .getOrCreate();

        //TODO: create a new RfqProcessor and set it listening for incoming RFQs
        /*
        RfqProcessor rfqProc = new RfqProcessor(session,jssc);
        rfqProc.startSocketListener();
        */

        KafkaRfqProcessor processor = new KafkaRfqProcessor(session, jssc);
        JavaDStream<String> stream = processor.initRfqStream();
        RfqProcessor rProc = new RfqProcessor(session, jssc);

        stream.foreachRDD(rdd -> {
            rdd.collect().stream().forEach(i -> rProc.processRfq(Rfq.fromJson(i)));

        });

        System.out.println("Listening for RFQs");
        jssc.start();
        jssc.awaitTermination();

    }

}
