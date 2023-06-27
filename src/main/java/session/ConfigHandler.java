package session;

import java.io.File;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

public class ConfigHandler {
    private Config conf;

    public ConfigHandler(String filepath, String env) {
        File file = new File(filepath);
        try {
            conf = ConfigFactory.parseFile(file).getConfig(env)
                    .withFallback(ConfigFactory.parseFile(file).getConfig("application"));
        } catch (ConfigException e) {
            System.out.println("Error: " + e);
        }
    }

    public Config getConfig() {
        return conf;
    }
}
