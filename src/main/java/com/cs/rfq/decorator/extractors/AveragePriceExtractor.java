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

        // find the sum of all unit prices
        String query = String.format("SELECT sum(LastPx) from trade where SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                tradesPastWeek);

        Dataset<Row> sqlQueryResults = session.sql(query);
        Double totalPrice = (Double) sqlQueryResults.first().get(0);

        //Double avgPrice = totalPrice / tradesPastWeek;

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(averagePriceTradedByEntityPastWeek, 0);
        return results;
    }
}
