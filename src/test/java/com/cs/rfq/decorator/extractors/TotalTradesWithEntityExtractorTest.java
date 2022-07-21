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
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TotalTradesWithEntityExtractorTest extends AbstractSparkUnitTest {
    String string;
    Rfq rfq;
    Dataset<Row> data;
    Dataset<Row> trades;

    @BeforeEach public void setUp() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000383864");

        String filePath = "src/test/resources/trades/trades.json";
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void extractTotalTradesWithEntityByDate() {
        TotalTradesWithEntityExtractor extractor = new TotalTradesWithEntityExtractor();


        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object year = meta.get(RfqMetadataFieldNames.tradesWithEntityPastYear);
        Object yearToDate = meta.get(RfqMetadataFieldNames.volumeTradedYearToDate);
        Object month = meta.get(RfqMetadataFieldNames.tradesWithEntityPastMonth);
        Object week = meta.get(RfqMetadataFieldNames.tradesWithEntityPastWeek);
        Object today = meta.get(RfqMetadataFieldNames.tradesWithEntityToday);

        assertEquals(68L, year);
        assertEquals(0L, week);
        assertEquals(2L, month);
        assertEquals(0L, today);

    }
}
