package session;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;

import java.util.HashMap;
import java.util.Map;
import session.DataStructures.*;


public class WriteStreamHandler implements VoidFunction2<Dataset<Session>, Long> {

    private final String format;
    private final String path;
    private final String mode;
    private final Map<String, String> optionsMap;

    public WriteStreamHandler(Config loadConfig) {

        Map<String, String> optionsMap = new HashMap<>();
        for (Map.Entry<String, ConfigValue> entry : loadConfig.getConfig("options").entrySet()) {
            if (!entry.getKey().equals("schema"))
                optionsMap.put(entry.getKey(), entry.getValue().unwrapped().toString());
        }

        this.format = loadConfig.getString("format");
        this.path = loadConfig.getString("path");
        this.mode = loadConfig.getString("mode");
        this.optionsMap = optionsMap;
    }

    @Override
    public void call(Dataset<Session> batchDF, Long batchId)  {
        if (!batchDF.isEmpty()) {
            batchDF.show();
            batchDF.write()
                    .format(format)
                    .options(optionsMap)
                    .mode(mode)
                    .save(path);
        }
    }
}
