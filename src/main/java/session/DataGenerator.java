package session;

import org.apache.spark.sql.execution.streaming.MemoryStream;
import scala.collection.JavaConverters;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class DataGenerator implements Runnable {
    private final MemoryStream<String> memStream;
    private final Thread dataGenerationThread;

    public DataGenerator(MemoryStream<String> memStream) {
        this.memStream = memStream;
        this.dataGenerationThread = new Thread(this);
    }

    public void startGeneratingData() {
        dataGenerationThread.start();
    }

    @Override
    public void run() {
        Random rand = new Random();
        List<Integer> userIDList = Arrays.asList(124, 125, 126, 127);
        List<String> eventTypeList = Arrays.asList("SessionStart", "SessionEnd");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime time = LocalDateTime.now();

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            int userID = userIDList.get(rand.nextInt(userIDList.size()));
            int sessionID = rand.nextInt(3) + 1;
            String eventType = eventTypeList.get(rand.nextInt(eventTypeList.size()));

            time = time.plusSeconds(1);
            String timestamp = time.format(formatter);

            String data = String.format("%d,%d,%s,%s", userID, sessionID, eventType, timestamp);
            memStream.addData(JavaConverters.asScalaBuffer(Collections.singletonList(data)).toSeq());
        }
    }
}

