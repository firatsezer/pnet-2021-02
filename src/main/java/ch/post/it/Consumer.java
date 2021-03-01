package ch.post.it;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

@Slf4j
@Component
public class Consumer {


    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    @Value("${POST_TIME_OUT_SEC:30}")
    private int post_time_out_sec;

    @Value("${REST_ENDPOINT_URL}")
    private String rest_endpoint_url;


    @KafkaListener
            (id = "pnet.consumer.1",
                    topicPartitions = {
                            @TopicPartition
                                    (
                                            topic = "${KAFKA_TOPICS}",
                                            partitions = {"${PARTITION}"}
                                    )
                    },
                    groupId = "${KAFKA_GROUP_ID}",
                    containerFactory = "listenerContainerFactory",
                    autoStartup = "false")
    public void consumeFromPartition(@Payload ConsumerRecords<String, String> records,
                                     Acknowledgment ack) {
        log.info("CONSUME RECORDS COUNT: " + records.count());

        try {
            if (executePost(records) == 200) {

                // COMMIT OFFSET
                ack.acknowledge();

                // SICH SELBST STOPPEN
                MessageListenerContainer listenerContainer =
                        endpointRegistry.getListenerContainer("pnet.consumer.1");
                if (listenerContainer.isRunning()) {
                    log.info("STOP CHANNEL");
                    listenerContainer.stop();
                }
            }
        } catch (IOException | InterruptedException e) {
            log.error("FEHLER BEIM SENDEN DER DATEN AN DEN REST_ENDPOINT: {}", rest_endpoint_url, e);
        }
    }


    @Scheduled(cron = "${START_CHANNEL}")
    private void start_CHANNEL() {

        MessageListenerContainer listenerContainer =
                endpointRegistry.getListenerContainer("pnet.consumer.1");
        if (!listenerContainer.isRunning()) {
            log.info("START CHANNEL");
            listenerContainer.start();
        }
    }

    private int executePost(ConsumerRecords<String, String> records) throws IOException, InterruptedException {

        JSONArray arr = new JSONArray();

        for (ConsumerRecord<String, String> entry : records) {
            arr.put(entry.value());
        }


        JSONObject collection = new JSONObject();

        collection.put("ChargeableItemCollection", arr);

        HttpClient client = HttpClient.newHttpClient();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(rest_endpoint_url))
                .timeout(Duration.ofSeconds(post_time_out_sec))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(collection.toString(4))) // toString(4) == pretty_print
                .build();

        HttpResponse<String> response =
                client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.statusCode();
    }
}