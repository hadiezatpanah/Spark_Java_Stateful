package session;

import com.typesafe.config.Config;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.types.StructType;
import static  org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;

public class ApplySchema {
    Dataset<Row> DFWithScheme;

    public ApplySchema(MemoryStream inputMemStreamDF) {
        DFWithScheme = inputMemStreamDF.toDF()
                .selectExpr(
                        "cast(split(value,',')[0] as string) as userID",
                        "cast(split(value,',')[1] as string) as sessionID",
                        "cast(split(value,',')[2] as string) as eventType",
                        "to_timestamp(cast(split(value,',')[3] as string), 'yyyy-MM-dd HH:mm:ss') as timestamp"
                );
    }

    public ApplySchema(Dataset<Row> inputStreamDF, Config config, SparkSession spark) {
        config.getString("schema");
        String jsonString = config.getString("schema.json");


        DFWithScheme = inputStreamDF
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .select(from_json(new Column("value"), getSchema(spark, jsonString)).as("data"), col("key"))
                .withColumn("timestamp",to_timestamp(col("data.timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
                .selectExpr("data.userID AS userID", "data.sessionID AS sessionID" , "data.eventType AS eventType",
                        "timestamp AS timestamp");
    }
    private static StructType getSchema(SparkSession spark, String jsonString) {
        List<String> jsonData = Arrays.asList(jsonString);
        Dataset<String> dataset = spark.createDataset(jsonData, Encoders.STRING());
        Dataset<Row> dfWithSchema = spark.read().json(dataset);
        return dfWithSchema.schema();
    }

    public Dataset<Row> getDFWithScheme() {
        return DFWithScheme;
    }
}
