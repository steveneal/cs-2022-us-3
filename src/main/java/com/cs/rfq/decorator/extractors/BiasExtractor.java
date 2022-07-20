package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;
import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class BiasExtractor implements RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        // initializing time interval

        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        long pastMonthMs = DateTime.now().withMillis(todayMs).minusWeeks(4).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()))
                .filter(trades.col("TraderID").equalTo(rfq.getTraderId()))
                .filter(trades.col("TradeDate").$greater(new java.sql.Date(pastMonthMs)));

        filtered.show();

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();

        // If past month has no trades, return -1
        if (filtered.isEmpty()) {
            results.put(biasByMonth,-1);
            results.put(biasByWeek,-1);
            return results;
        }

        double biasByMonthAns;
        double biasByWeekAns;

        biasByMonthAns = (double) filtered
                .filter((filtered.col("Side").equalTo(1)))
                .count()
                /(double) filtered.count();



        results.put(biasByMonth,biasByMonthAns);
        Dataset<Row> filteredByWeek = filtered
                .filter(filtered.col("TradeDate").$greater(new java.sql.Date(pastWeekMs)));

        if(filteredByWeek.isEmpty()) {
            results.put(biasByMonth,biasByMonthAns);
            results.put(biasByWeek,-1);
            return results;
        }



        biasByWeekAns = (double) filteredByWeek
                .filter(filteredByWeek.col("Side").equalTo(1)).count()/
                (double) filteredByWeek.count();


        results.put(biasByWeek,biasByWeekAns);
        return results;
    }
}
