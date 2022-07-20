package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AveragePriceExtractorTest {
    Rfq rfq;
    Dataset<Row> trades;

    SparkSession session;

    @BeforeEach
    public void setUp() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000383864");

        String filePath = getClass().getResource("avgPrice-traded-1.json").getPath();
        session = SparkSession.builder()
                .appName("BiasExtractorTestSession")
                .master("local")
                .getOrCreate();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void testAveragePriceExtractor() {
        AveragePriceExtractor extractor = new AveragePriceExtractor();
        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object avgPrice = meta.get(RfqMetadataFieldNames.averagePriceTradedByEntityPastWeek);



        assertEquals(138.4396D, avgPrice);
    }
}
