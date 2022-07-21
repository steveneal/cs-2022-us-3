package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.cs.rfq.decorator.extractors.AbstractSparkUnitTest.session;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class VolumeTradedWithEntityTest {
    Rfq rfq;
    Dataset<Row> trades;

    SparkSession session;

    @BeforeEach
    public void setUp() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000383864");

        String filePath = "src/test/resources/trades/trades.json";
        session = SparkSession.builder()
                .appName("BiasExtractorTestSession")
                .master("local")
                .getOrCreate();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }
    @Test
    public void testVolumeTradedWithEntity() {
        VolumeTradedWithEntity extractor = new VolumeTradedWithEntity();


        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object month = meta.get(RfqMetadataFieldNames.volumeTradedWithEntityPastMonth);
        Object week = meta.get(RfqMetadataFieldNames.volumeTradedWithEntityPastWeek);
        Object year = meta.get(RfqMetadataFieldNames.volumeTradedWithEntityPastYear);

        assertEquals(729L, year);
        assertEquals(13L, week);
        assertEquals(48L, month);
    }
}
