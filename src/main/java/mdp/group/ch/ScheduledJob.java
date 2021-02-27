package mdp.group.ch;

import lombok.extern.slf4j.Slf4j;
import mdp.group.ch.entitys.DataCollection;
import mdp.group.ch.entitys.Log;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Date;

@Slf4j
@Component
public class ScheduledJob {

    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    @Autowired
    private DataRepository dataRepository;

    @Autowired
    private LogRepository logRepository;


    @Value("${REST_ENDPOINT_URL}")
    private String targetURL;

    @Value("${POST_TIME_OUT:30}")
    private int timeout;


    @Scheduled(cron = "${CRON_JOB}")
    private void executePost() throws IOException, InterruptedException {

        MessageListenerContainer listenerContainer =
                endpointRegistry.getListenerContainer("fooGroup1");


        if (listenerContainer.isRunning()) {
            log.info("STOP LISTENER");
            listenerContainer.stop();
        } else {
            log.info("START LISTENER");
            listenerContainer.start();
        }

        if (dataRepository.count() <= 0) {
            return;
        }


        final int min = dataRepository.findMIN();
        final int max = dataRepository.findMAX();

        final Iterable<DataCollection> all = dataRepository.findAll();
        JSONArray arr = new JSONArray();
        all.forEach((dataCollection) -> {
            JSONObject jsonObject = new JSONObject(dataCollection.getData());
            arr.put(jsonObject);
        });

        JSONObject collection = new JSONObject();

        collection.put("ChargeableItemCollection", arr);

        log.info("\nPOSTDATA Server: " + targetURL + ",\nITEMS: " + arr.length());

        HttpClient client = HttpClient.newHttpClient();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(targetURL))
                .timeout(Duration.ofSeconds(timeout))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(collection.toString(4)))
                .build();


        HttpResponse<String> response =
                client.send(request, HttpResponse.BodyHandlers.ofString());
        log.info(String.valueOf(response.statusCode()));
        if (response.statusCode() == 200) {
            Log log = new Log();
            log.setDatetime(new Date());
            log.setInfo("POSTED OFFSETS FROM : " + min + " TO : " + max);
            logRepository.save(log);
            all.forEach((dataCollection) -> {
                dataRepository.delete(dataCollection);
            });
        }

    }
}
