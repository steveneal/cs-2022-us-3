package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class InstrumentLiquidity implements RfqMetadataExtractor{

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        String since = DateTime.now().getYear() + "-";
        int toAdd = DateTime.now().getMonthOfYear();

        if(toAdd<10){
            since+="0";
        }
        since+=toAdd +"-01";

        String query = String.format("SELECT sum(LastQty) from trade where SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object volume = sqlQueryResults.first().get(0);
        if (volume == null) {
            volume = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.instrumentLiquidity,volume);

        return results;
    }
}
