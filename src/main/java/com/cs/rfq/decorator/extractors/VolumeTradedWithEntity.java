package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;
import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.tradesWithEntityPastMonth;

public class VolumeTradedWithEntity implements RfqMetadataExtractor {
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        long pastYearMs = DateTime.now().withMillis(todayMs).minusYears(1).getMillis();
        long pastMonthMs = DateTime.now().withMillis(todayMs).minusWeeks(4).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));

        long tradesPastWeek = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastWeekMs))).count();
        long tradesPastYear = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastYearMs))).count();
        long tradesPastMonth = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastMonthMs))).count();

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(volumeTradedWithEntityPastWeek, tradesPastWeek);
        results.put(volumeTradedWithEntityPastYear, tradesPastYear);
        results.put(volumeTradedWithEntityPastMonth, tradesPastMonth);
        return results;
    }
}
