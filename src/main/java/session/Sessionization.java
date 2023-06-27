package session;

import com.typesafe.config.Config;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.*;


import session.DataStructures.*;


public final class Sessionization {
    public static void main(String[] args) throws Exception {

        String filepath = args[0];
        String env = args[1];
        ConfigHandler configHandler = new ConfigHandler(filepath, env);
        Config config = configHandler.getConfig();

        SparkSession spark = new SparkSessionHandler(config.getConfig("SparkSession")).getSparkSession();
        InputStreamHandler inputStreamHandler = new InputStreamHandler(config.getConfig("Extract"), spark);
        Dataset<Row> dataFrameWithSchema;

        if (env.equals("dev")) {
            MemoryStream<String> inputMemStreamDF = inputStreamHandler.getInputMemStreamDF();
            dataFrameWithSchema = new ApplySchema(inputMemStreamDF).getDFWithScheme();

        } else {
            Dataset<Row> inputStreamDF = inputStreamHandler.getInputStreamDF();
            dataFrameWithSchema = new ApplySchema(inputStreamDF, config.getConfig("Extract.session"), spark).getDFWithScheme();
        }

        SessionUpdate stateUpdateFunc = new SessionUpdate(config.getConfig("Transform"));

        Dataset<Session> sessionUpdates = dataFrameWithSchema
                .groupByKey((MapFunction<Row, CompositeKey>) event -> new CompositeKey(event.getString(0), event.getString(1)), Encoders.bean(CompositeKey.class))
                .flatMapGroupsWithState(
                        stateUpdateFunc,
                        OutputMode.Append(),
                        Encoders.bean(Sessions.class),
                        Encoders.bean(Session.class),
                        GroupStateTimeout.ProcessingTimeTimeout());


        Config loadOptionConfig = config.getConfig("Load.options");
        final String ProcessingTime = loadOptionConfig.getString("trigger.processingTime ");
        final String outputMode = loadOptionConfig.getString("output.mode");
        final String checkpointLocation = loadOptionConfig.getString("checkpointLocation");
        Config loadConfig = config.getConfig("Load.output");

        WriteStreamHandler writeStreamHandler = new WriteStreamHandler(
                loadConfig
        );


        StreamingQuery query = sessionUpdates
                .writeStream()
                .option("checkpointLocation", checkpointLocation)
                .trigger(Trigger.ProcessingTime(ProcessingTime))
                .outputMode(outputMode)
                .foreachBatch(writeStreamHandler)
                .start();

        query.awaitTermination();

    }

}