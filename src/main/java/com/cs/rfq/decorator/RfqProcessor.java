package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.*;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Console;
import java.util.*;

import static org.apache.spark.sql.functions.sum;

public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private final JavaStreamingContext streamingContext;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext) {
        this.session = session;
        this.streamingContext = streamingContext;

        //TODO: use the TradeDataLoader to load the trade data archives
        TradeDataLoader tload = new TradeDataLoader();
        trades = tload.loadTrades(session,"src/test/resources/trades/trades.json");

        //TODO: take a close look at how these two extractors are implemented
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new VolumeTradedWithEntityYTDExtractor());
        extractors.add(new InstrumentLiquidityExtractor());
        extractors.add(new AveragePriceExtractor());
        extractors.add(new VolumeTradedWithEntity());


    }

    public void startSocketListener() throws InterruptedException {
        //TODO: stream data from the input socket on localhost:9000
        JavaDStream<String> lines = streamingContext.socketTextStream("localhost", 9000);
        //TODO: convert each incoming line to a Rfq object and call processRfq method with it
        //not sure if above lines converted input into json format so will need ot check that
        lines.foreachRDD(rdd -> {
            rdd.collect().forEach(line -> processRfq(Rfq.fromJson(line)));
        });


        //TODO: start the streaming context
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void processRfq(Rfq rfq) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));

        //create a blank map for the metadata to be collected
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();

        //TODO: get metadata from each of the extractors
        for(int i=0;i<extractors.size();i++){
            metadata.putAll(extractors.get(i).extractMetaData(rfq,session,trades));
        }

        //TODO: publish the metadata
        publisher.publishMetadata(metadata);
        System.out.println("Do you want to accept the trade? Enter Yes or No.");
        Scanner console = new Scanner(System.in);
        String input = console.next();
        if (input.toUpperCase().equals("YES")) {
            System.out.println("You have accepted the trade.");
        } else {
            System.out.println("You have rejected the trade.");
        }

    }
}
