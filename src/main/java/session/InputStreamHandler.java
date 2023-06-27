package session;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.DataStreamReader;
import java.util.*;

public class InputStreamHandler {
    private MemoryStream<String> inputMemStreamDF;
    private Dataset<Row> inputStreamDF;

    public InputStreamHandler(Config config, SparkSession sparkSession) {
        if (config.getString("sessions.format").equals("MemoryStream")) {
            inputMemStreamDF = new MemoryStream<String>(42, sparkSession.sqlContext(), null, Encoders.STRING());
            DataGenerator dataGenerator = new DataGenerator(inputMemStreamDF);
            dataGenerator.startGeneratingData();

        } else {
            DataStreamReader reader = sparkSession.readStream().format("kafka");

            Map<String, String> optionsMap = new HashMap<>();
            for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
                if (!entry.getKey().equals("schema"))
                    optionsMap.put(entry.getKey(), entry.getValue().unwrapped().toString());
            }

            inputStreamDF = reader.options(optionsMap).load();
        }
    }


    public MemoryStream<String> getInputMemStreamDF() {
        return inputMemStreamDF;
    }

    public Dataset<Row> getInputStreamDF() {
        return inputStreamDF;
    }
}
