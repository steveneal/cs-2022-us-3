package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Date;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TradeDataLoaderTest extends AbstractSparkUnitTest {

    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        String filePath = getClass().getResource("loader-test-trades.json").getPath();
        SparkSession session = SparkSession.builder()
                .appName("TradeDataLoaderTestSession")
                .master("local")
                .getOrCreate();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void loadTradeRecords() {
        // trades.show();
        assertEquals(5, trades.count());
    }

    @Test
    public void loadTradeFields() {
        assertEquals(5, trades.count());

        Long traderId = trades.first().getLong(0);
        Long entityId = trades.first().getLong(1);
        String securityId = trades.first().getString(5);
        Long lastQty = trades.first().getLong(7);
        Double lastPx = trades.first().getDouble(8);
        Date tradeDate = trades.first().getDate(9);
        String currency = trades.first().getString(14);

        //2018-06-09
        Date expectedTradeDate = new Date(new DateTime().withYear(2018).withMonthOfYear(6).withDayOfMonth(9).withMillisOfDay(0).getMillis());

        assertAll(
                () -> assertEquals((Long) 7704615737577737110L, traderId),
                () -> assertEquals((Long) 5561279226039690843L, entityId),
                () -> assertEquals("AT0000A0VRQ6", securityId),
                () -> assertEquals((Long) 500000L, lastQty),
                () -> assertEquals((Double) 139.648, lastPx),
                () -> assertEquals(expectedTradeDate, tradeDate),
                () -> assertEquals("EUR", currency)
        );
    }
}
