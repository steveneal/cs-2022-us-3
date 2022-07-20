package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class TotalTradesWithEntityExtractorTest extends AbstractSparkUnitTest {
    String string;
    Rfq rfq;
    Dataset<Row> data;
    SparkSession session;

    @BeforeEach public void setUp() {
        string = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000383864', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        rfq = Rfq.fromJson(string);

        String filePath = getClass().getResource("test-trades.json").getPath();
        session = SparkSession.builder()
                .appName("TotalTradesByEntitySession")
                .master("local")
                .getOrCreate();
        data = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void extractTotalTradesWithEntityByDate() {
        TotalTradesWithEntityExtractor extractorByDay = new TotalTradesWithEntityExtractor();
        Map<RfqMetadataFieldNames, Object> output = extractorByDay.extractMetaData(rfq,session,data);
        System.out.println(output);
        // To-Do: implement unit test
    }
}
