package com.demo.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;

public class StringConsumer extends Thread {

    private String topic = "cliettwopartitions3";

    private String groupId = "cliettwopartitions3";

    private long endingOffset;
    private KafkaConsumer<String, String> kafkaConsumer;

    public StringConsumer(long endingOffset) {
        this.endingOffset = endingOffset;
    }

    public void run() {
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "offset123");
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        ConsumerRecords<String, String> records1 = kafkaConsumer.poll(100);

        Set<TopicPartition> partitions = kafkaConsumer.assignment();

        System.out.println("Current position before seekToBeginning is: "+kafkaConsumer.position(new ArrayList<TopicPartition>(partitions).get(0)));
        System.out.println("Current commited position before seekToBeginning is: "+kafkaConsumer.committed(new ArrayList<TopicPartition>(partitions).get(0)));

        kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());

        System.out.println("Current position after seekToBeginning is: "+kafkaConsumer.position(new ArrayList<TopicPartition>(partitions).get(0)));
        System.out.println("Current commited position after seekToBeginning is: "+kafkaConsumer.committed(new ArrayList<TopicPartition>(partitions).get(0)));


//        //Start processing messages
        try {
            boolean keepOnReading = true;
            while (keepOnReading) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Key: "+record.key() + ", Value: " + record.value());
                    System.out.println("Partition: " + record.partition() + ", Offset: " + record.offset());

                    if(record.offset() == endingOffset) {
                        keepOnReading = false;
                        break;
                    }
                }
            }
        } catch(WakeupException ex){
            System.out.println("Exception caught " + ex.getMessage());
        } finally{
            kafkaConsumer.close();
            System.out.println("After closing KafkaConsumer");
        }
    }

    public static void main(String[] args) {
        StringConsumer stringConsumer = new StringConsumer(6l);
        stringConsumer.start();
    }

}
