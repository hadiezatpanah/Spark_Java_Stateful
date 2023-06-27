package session;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import java.util.Map;

public class SparkSessionHandler {
    private SparkSession sparkSession;

    public SparkSessionHandler(Config config) {
        SparkConf sparkConf = new SparkConf();

        for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue().unwrapped();
            sparkConf.set(key, value.toString());
        }

        if (config.getString("spark.master").contains("local")) {
            sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        } else {
            sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        }

        sparkSession.sparkContext().setLogLevel("ERROR");

    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }
}