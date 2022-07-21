package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;


import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BiasExtractorTest extends AbstractSparkUnitTest {
    String string;
    Rfq rfq;
    Dataset<Row> data;
    SparkSession session;

    @BeforeEach public void setUp() {
        string = "{" +
                "'id': '123ABC', " +
                "'traderId': 7704615737577737110, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000A0VRQ6', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        rfq = Rfq.fromJson(string);

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        session = SparkSession.builder()
                .appName("BiasExtractorTestSession")
                .master("local")
                .getOrCreate();
        data = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void biasExtractorTest() {
        // To-Do: implement unit test
        BiasExtractor testBiasExtractor = new BiasExtractor();
        Map<RfqMetadataFieldNames, Object> output = testBiasExtractor.extractMetaData(rfq,session,data);
        assertEquals(1.0,output.get(biasByMonth));
        assertEquals(1.0, output.get(biasByWeek));
    }
}