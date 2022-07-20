package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.sql.Date;
import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;
import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.tradesWithEntityPastMonth;
import static org.apache.spark.sql.functions.avg;

public class AveragePriceExtractor implements RfqMetadataExtractor {
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        // security id = instrument id
        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));

        long tradesPastWeek = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastWeekMs))).count();
        Dataset<Row> avgPriceSet = filtered.agg(avg(filtered.col("LastPx")));
        Double avgPrice = avgPriceSet.first().getDouble(0);

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(averagePriceTradedByEntityPastWeek, avgPrice);
        return results;
    }
}
