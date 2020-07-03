package com.demo.engine;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class ChangeOffsets extends Thread {

    private String topic = "cliettwopartitions3";

    private String groupId = "cliettwopartitions3";

    private long endingOffset = 3l;

    private KafkaConsumer<String, String> kafkaConsumer;

    private Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    public void run()  {
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "offset123");
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);



        kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        kafkaConsumer.poll(100);

        Set<TopicPartition> partitions = kafkaConsumer.assignment();
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(endingOffset);

        for (TopicPartition partition : kafkaConsumer.assignment()) {
            offsets.put(partition, offsetAndMetadata);
        }

        kafkaConsumer.commitSync(offsets);

    }

    public static void main(String[] args) {
        ChangeOffsets changeOffsets = new ChangeOffsets();
        changeOffsets.start();
    }


}
