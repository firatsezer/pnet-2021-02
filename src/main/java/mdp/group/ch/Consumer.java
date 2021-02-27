package mdp.group.ch;


import mdp.group.ch.entitys.DataCollection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
public class Consumer {

    private final Logger logger = Logger.getLogger(Consumer.class.getName());


    @Autowired
    DataRepository repo;

    //    @KafkaListener(id = "fooGroup", autoStartup = "false", topics = "${KAFKA_TOPICS}", groupId = "${KAFKA_GROUP_ID}")
    public void consume(@Payload ConsumerRecord<String, String> message) throws IOException {

        logger.info("Consum Message with OFFSET :" + message.offset());

        DataCollection dc = new DataCollection();
        dc.setOffset(message.offset());
        dc.setData(message.value());
        repo.save(dc);

    }

    @KafkaListener
            (
                    id = "pnet.consumer.1",
                    topicPartitions =
                            {
                                    @TopicPartition(topic = "blabla", partitions = {"0"})
                            },
                    groupId = "group_one",
                    containerFactory = "listenerContainerFactory",
                    autoStartup = "false"
            )
    public void consumeFromPartition(@Payload ConsumerRecords<String, String> records,
                                     Acknowledgment acknowledgment) {
        logger.info("CONSUME RECORDS COUNT: " + records.count());


        try {
            executePost(records);
            acknowledgment.acknowledge();
        } catch (IOException | InterruptedException e) {
            logger.log(Level.SEVERE, null, e);
        }
    }

    private void executePost(ConsumerRecords<String, String> records) throws IOException, InterruptedException {

        JSONArray arr = new JSONArray();

        for (ConsumerRecord<String, String> entry : records) {
            arr.put(entry.value());
        }


        JSONObject collection = new JSONObject();

        collection.put("ChargeableItemCollection", arr);


        HttpClient client = HttpClient.newHttpClient();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://requestinspector.com/inspect/01ezf2yksezh21e628ax8azxqs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(collection.toString(4)))
                .build();


        HttpResponse<String> response =
                client.send(request, HttpResponse.BodyHandlers.ofString());
    }

}