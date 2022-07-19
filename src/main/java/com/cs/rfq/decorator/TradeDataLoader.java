package com.cs.rfq.decorator;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.types.DataTypes.*;

public class TradeDataLoader {

    private final static Logger log = LoggerFactory.getLogger(TradeDataLoader.class);

    public Dataset<Row> loadTrades(SparkSession session, String path) {

        StructType schema = new StructType (new StructField[] {
                DataTypes.createStructField("TraderId", LongType,false),
                DataTypes.createStructField("EntityId", DataTypes.LongType,false),
                DataTypes.createStructField("MsgType", IntegerType,false),
                DataTypes.createStructField("TradeReportId", LongType,false),
                DataTypes.createStructField("PreviouslyReported", DataTypes.StringType,false),
                DataTypes.createStructField("SecurityID", DataTypes.StringType,false),
                DataTypes.createStructField("SecurityIdSource", IntegerType, false),
                DataTypes.createStructField("LastQty", LongType, false),
                DataTypes.createStructField("LastPx", DataTypes.DoubleType, false),
                DataTypes.createStructField("TradeDate", DataTypes.DateType, false),
                DataTypes.createStructField("TransactTime", StringType, false),
                DataTypes.createStructField("NoSides", IntegerType, false),
                DataTypes.createStructField("Side", DataTypes.IntegerType, false),
                DataTypes.createStructField("OrderID", LongType, false),
                DataTypes.createStructField("Currency", DataTypes.StringType, false)
        });


        Dataset<Row> trades = session
                .read()
                .schema(schema)
                .json(path);


        log.info("Loaded "+ trades.count() + "rows; Schema used: " + schema);

        return trades;
    }

}
